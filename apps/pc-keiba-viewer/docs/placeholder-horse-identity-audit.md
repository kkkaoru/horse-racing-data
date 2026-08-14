# Placeholder horse identity audit

## Scope and rule

Audit of every `ketto_toroku_bango` reference in `src/db/queries.ts` after the overseas-runner incident.

An empty or all-zero registration number does not identify a horse. Race-entry display data may still use the row, but cross-race horse history, aggregation, ranking, favorites, and detail routing must not use the placeholder as an identity.

## Fixed

| Query                                                    | Risk                                                                             | Resolution                                                                         |
| -------------------------------------------------------- | -------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `getHorseList` recent and filtered JRA/NAR paths         | All placeholder entries merged into one phantom horse and one aggregate          | Exclude empty and trimmed all-zero IDs                                             |
| `searchFavoriteHorses` recent and fallback JRA/NAR paths | `/horses/000…` could become a favorite candidate                                 | Exclude empty and trimmed all-zero IDs                                             |
| `getHorseRaceResults` JRA/NAR history                    | Every placeholder runner received the union of other placeholder histories       | Exclude placeholders from `current_horses` before both history branches            |
| `getRaceAbilityTests`                                    | NAR ability-test histories could merge through the placeholder                   | Exclude empty and trimmed all-zero current IDs                                     |
| `getTimeScoreRows` JRA/NAR history                       | Time-score histories could merge through the placeholder                         | Exclude placeholders from `current_horses`                                         |
| `getRaceTrainings`                                       | Ranking by registration number collapsed multiple no-workout placeholder runners | Partition current-race workout rows by `umaban` and training type                  |
| `getRaceTimeStats.current_horse_stats`                   | Placeholder runners could receive unrelated horse-history statistics             | Keep the race-entry row but prevent its history join when the ID is empty/all-zero |

## No change required

| Query                                                               | Reason                                                                                                                                                                                       |
| ------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `getRaceRunners`                                                    | Reads race-entry rows and horse masters only. A placeholder has no master match; overseas enrichment joins by `race_source + race key + umaban`, not by the placeholder.                     |
| `entityRaceRowsSql` / `getEntityResultRows` / `getPersonResultRows` | The registration number is projected on independent race-entry rows. Person aggregation uses person names and does not join horse history through the number.                                |
| `getBloodlineStats`                                                 | Registration numbers are used to resolve horse masters. A placeholder has no master match, becomes bloodline `不明`, and is removed by the target-name filter before historical aggregation. |

## Remaining fixes

| Query                 | Risk                                                                                                             | Planned resolution                                                                                             |
| --------------------- | ---------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `getHorseDetailData`  | Direct access to `/horses/000…` can still merge every placeholder row even though generated UI links are blocked | Reject a trimmed all-zero detail ID before querying; retain normal IDs                                         |
| `getSimilarRaceStats` | `count(distinct ketto_toroku_bango)` counts all historical placeholder entries as one horse                      | Use the real registration number when available and a source/race-key/`umaban` entry identity for placeholders |

## Status

The remaining fixes were deferred when production prediction recovery became the higher-priority incident. No item is awaiting classification.
