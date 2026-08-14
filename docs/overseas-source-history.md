# Overseas source histories

## Provenance

`oversea_horse_race_history` intentionally stores source-native rows from both `jra-van` and `netkeiba`. The same real-world race can therefore appear once per source. Consumers must select a source; they must not aggregate both sources as independent starts.

For the 2026 Prix Jacques le Marois viewer path, `getHorseRaceResults` explicitly selects `mapping.source = 'netkeiba'` for placeholder overseas runners. Runners with real JV registration numbers continue to use JV history. This prevents source duplication and duplication between supplemental and JV histories.

The viewer change has not been deployed. Deployment is deferred until after the 2026-08-15 race-day operations window.

A production data sync on 2026-08-15 warmed the 2026-08-16 race-detail section and SSR caches while the deployed viewer still lacked this history path. Before deploying a history-shape change, confirm that its DB-query and race-detail cache namespaces differ from those used by an earlier warm. Deployment alone does not invalidate existing cache keys.

### Cache invalidation incident

The 2026 Prix Jacques le Marois exposed multiple cache layers with different invalidation behavior:

- detail-section payloads are stored in both `DETAIL_SECTION_CACHE_KV` and `caches.default` (the Workers Cache API); deleting the KV main and stale keys does not delete the Cache API entry;
- Cache API entries use an internal `pc-keiba-viewer.local/detail-section-cache/...` request URL. Cloudflare does not support purge-by-public-URL for a Worker-defined custom cache key;
- broad host, prefix, or purge-everything operations would affect unrelated races, and the available operator token had zone read permission but no cache-purge permission;
- the production TTL extends through race start plus six hours, so waiting for expiration can preserve an incorrect pre-warm snapshot through the race;
- `race-cache-bust` deletes KV keys only. Its generation key currently has no read consumer and therefore does not bypass an existing Cache API entry.

For a global, race-safe invalidation of a changed payload shape, change the relevant code-side cache version and deploy. The 2026 fix gave only alphanumeric overseas venues new versions for history-dependent detail sections and finish-prediction inputs; numeric JRA, NAR, and Ban-ei cache keys remained unchanged. A permanent follow-up should either make the generation key part of Cache API lookup keys or otherwise give `race-cache-bust` an effective Cache API invalidation path. Provisioning narrowly scoped cache-purge credentials remains an operator decision.

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

Thus a profile date is not a stable cross-source race identity, and the normalization direction differs by record. The More Thunder shift is explained by local date versus JST; the opposite Strauss shift remains unexplained and conflicts with netkeiba's own race page. Cross-source consumers must not use exact date alone as a join key.

## Person-result value

Of the stored recent rows, source race identifiers classified the following as overseas rather than domestic JV-shaped races:

- jockeys: 174/200 rows across 9 people;
- trainers: 136/176 rows across 8 people;
- owners: 58/78 rows across 5 owners.

This is the information not reliably available from domestic JV histories, including all 20 stored results for C. Lee, who has no resolved JV jockey code in the target race.

The 454 source-person rows are recent profile lists, not a complete and consistently timed statistical population. They are suitable for race-list display but are not used to calculate a comparable win rate. For `A8`, the win-rate tables retain the existing JV exact-name aggregation and broaden only the structurally impossible venue/condition scope: the default is the preceding ten years across all Japanese venues and conditions. The API returns `similarStatsFallback=true`, and the UI discloses that scope. Overseas rows with fewer than 20 JV starts are suppressed.

The resulting target coverage is nine of ten jockeys, five of ten trainers, and the shared Carrot Farm owner row for horses 3 and 4. C. Lee, four unmatched foreign trainers, S. Wattel with only three JV starts, and owners without JV histories remain empty. This is intentional: recent capped source rows are not substituted into the same percentage column with a different scale.

## Pedigree display completeness

Cached netkeiba pedigree AJAX responses supplied source-native IDs and names for sire, sire-sire, dam, and dam-sire for all ten runners. They are stored in `oversea_horse_pedigree`; they do not replace JV horse masters and are joined only through a complete `oversea_runner_source_id` race-entry mapping.

`getBloodlineStats` already identifies ancestors by exact pedigree name for domestic horses. Overseas placeholder runners use the same exact-name path: no alias, case-insensitive, partial, or fuzzy matching is added. A foreign ancestor need not have its own `jvd_um.ketto_toroku_bango`; a name absent from JV pedigree fields simply contributes no statistics. For overseas venues only, a target name resolving to more than one distinct JV horse code is excluded. This suppresses the ambiguous `キズナ` target rather than mixing two same-named identities.

Deauville (`A8`) cannot have a domestic same-venue population. Its default bloodline aggregation therefore sets `includeVenue=false` and returns `bloodlineVenueFallback=true`. The UI explains that the value uses JV/NAR results from all Japanese venues. The formula and category weights are unchanged. A category is eligible only with at least 20 starts: the existing reliability term reaches 20/30 at that threshold, while a ten-start row would supply only one third of its calibrated reliability input. Numeric JRA/NAR venue behavior is unchanged.

The final relaxed population is the prior ten years at 1600 metres, before 2026-08-16. Counts below are JV/NAR progeny starts used by the existing query; `0` means no exact match or fewer qualifying results. A JV code describes the ancestor itself, not the source-native external identity.

| Runner               | Sire (JV code; starts)                                   | Sire-sire (JV code; starts)              | Dam-sire (JV code; starts)                                |
| -------------------- | -------------------------------------------------------- | ---------------------------------------- | --------------------------------------------------------- |
| Zeus Olympios        | Night of Thunder (none; 0)                               | Dubawi (none; 2042)                      | Siyouni (none; 47)                                        |
| Dreamliner           | Adlerflug (none; 0)                                      | In the Wings (none; 0)                   | Aussie Rules (none; 0)                                    |
| Sixpence             | キズナ (`2010105827`, but same-name ambiguity; excluded) | ディープインパクト (`2002100816`; 14337) | Twirling Candy (none; 0)                                  |
| Strauss              | モーリス (`2011100655`; 1441)                            | スクリーンヒーロー (`2004103328`; 1703)  | アドマイヤベガ (`1996107396`; 876)                        |
| More Thunder         | Night of Thunder (none; 0)                               | Dubawi (none; 2042)                      | ハットトリック (`2001103018`; 0; no alias to `Hat Trick`) |
| No Lunch             | Dubawi (none; 64)                                        | Dubai Millennium (none; 64)              | Shamardal (none; 332)                                     |
| Sir Tommy Cen        | Dark Angel (none; 50)                                    | Acclamation (none; 51)                   | Haafhd (none; 0)                                          |
| Life                 | Sea The Moon (none; 0)                                   | Sea The Stars (none; 0)                  | Holy Roman Emperor (none; 156)                            |
| The Secret Adversary | St Mark's Basilica (none; 0)                             | Siyouni (none; 0)                        | Gleneagles (none; 0)                                      |
| Precise              | Starspangledbanner (none; 0)                             | Choisir (none; 0)                        | Galileo (none; 552)                                       |

Data-supported bloodline scores are available for six of the eight placeholder runners. Dreamliner and The Secret Adversary remain at the neutral `0.5` because none of their ancestor categories meet the threshold. This improves display completeness only; it does not retrain a model or claim improved prediction accuracy.
