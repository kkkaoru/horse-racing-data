# Overseas runner identity UI-consumption audit

Date: 2026-08-15 JST

## Scope

This is the follow-up audit for the defect pattern "the database query returns supplemental overseas identity fields, but a consumer continues to render fixed-width JV fields." The canonical display values are optional `Runner` fields populated by `getRaceRunners`:

- `horseNameFull`
- `jockeyNameFull`
- `trainerNameFull`
- `ownerNameFull`
- `identitySource`, `sourceHorseId`, and `sourceUrl`

## Closed primary path

`runners-table.tsx` now uses all four full-name fields with the JV value as fallback. Horse links use a real JV registration number when one exists, otherwise a safe supplemental profile URL. The 2026 Jacques le Marois local page was checked with ten profile identities and full people/owner names.

This is the only table in the initial server-rendered page that displays horse, jockey, trainer, and owner together, and it satisfies the reported user-facing issue.

## Other display-only consumers still using JV text

These consumers do not merge horse histories, but their labels can remain abbreviated or omit an owner even after supplemental data is present:

| Consumer                                                 | Current use                                       | Impact                                                           | Recommendation                                                     |
| -------------------------------------------------------- | ------------------------------------------------- | ---------------------------------------------------------------- | ------------------------------------------------------------------ |
| `race-detail-page.tsx` `baseProcessedData.runnerRows`    | `bamei`, abbreviated jockey/trainer, `banushimei` | AI JSON export contains Japanese/abbreviated names and `-` owner | Prefer supplemental display fields in a follow-up                  |
| `race-ai-data.ts` standard prompt rows                   | JV horse/jockey/trainer; no owner                 | AI prompt does not see canonical overseas identity               | Serialize supplemental fields in AI API and prefer them            |
| AI data route `pickRunner`                               | Drops every supplemental identity field           | Downstream AI cannot recover canonical names or profile URL      | Add optional fields without removing current keys                  |
| `paddock-section.tsx` runner rows                        | JV horse/jockey/trainer                           | Paddock labels remain abbreviated when paddock data exists       | Prefer supplemental display fields                                 |
| `realtime-race-section.tsx` odds labels                  | JV horse name                                     | Odds chart uses Japanese card label, not full profile name       | Prefer `horseNameFull`                                             |
| `detail-section-data.ts` display rows                    | JV horse/jockey labels                            | Overall-score/current-entry labels remain abbreviated            | Prefer supplemental fields only in output labels                   |
| `race-pace-prediction.ts` output label                   | JV horse name                                     | Pace output can show card label                                  | Prefer supplemental horse label after computation                  |
| `finish-position-prediction.ts` output label             | JV horse/jockey label                             | Prediction output can show abbreviated labels                    | Prefer supplemental fields only after matching/scoring             |
| current-runner labels in similar/bloodline/result tables | JV horse/jockey labels                            | Secondary tables can disagree with the runners table             | Prefer supplemental fields where the input is the current `Runner` |

These are presentation-consistency follow-ups, not a reason to delay the primary runners-table deploy. They should use one shared display helper to avoid another partial rollout.

## Consumers that must continue to use JV values for computation

Do **not** blindly replace every grep match. The following values are identity/join inputs into historical JV-derived statistics:

- `race-trend-payload.server.ts`: jockey/trainer names used to match trend aggregates;
- `race-pace-prediction.ts`: current jockey/trainer values used to select score rows;
- `finish-position-prediction.ts`: jockey/trainer equality and same-day score lookup;
- SQL/statistical functions whose historical tables contain only JV names.

Changing these to profile display names would make a full foreign name fail to match historical rows keyed by a JV abbreviation. Compute with the existing JV value, then substitute the supplemental value only in the final presentation object.

## Recommended implementation boundary

1. Add tested helpers for the four presentation names (`full -> JV -> fallback`).
2. Use them only at UI/export output boundaries.
3. Preserve raw JV fields in API output for compatibility and add supplemental fields alongside them.
4. Test a placeholder overseas runner and a domestic runner without supplemental data in every changed consumer.
5. Do not change score lookup, historical joins, or fixed-width JV columns.
