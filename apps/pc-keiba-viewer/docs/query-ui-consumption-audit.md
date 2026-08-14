# Query-to-UI consumption audit

Date: 2026-08-15 JST

## Scope

Follow-up to the overseas runner identity incident. This audit looks outside the supplemental overseas identity feature for the same two defect shapes:

1. a query returns a presentation value but an active UI/export path continues to use an older value; or
2. a presentation value is substituted into a field that is also used for matching, joining, or scoring.

The audit covered non-test references to aliases returned by `src/db/queries.ts`, fields in `src/lib/race-types.ts`, current-runner labels, realtime overrides, training/ability-test data, race-list filtering, and prediction/trend inputs.

## Result

No additional confirmed silent-display defect was found in the active non-overseas paths.

The overseas rollout leaves the important boundary explicit:

- current-runner presentation uses `getRunnerDisplayNames`;
- fixed-width JV jockey/trainer values remain inputs to historical matching, prediction scoring, and race-trend aggregation;
- raw API fields remain available for compatibility and supplemental fields are additive;
- realtime jockey data changes the displayed jockey only when it identifies a genuinely different jockey.

## Checked paths

| Area                       | Query/value that could be ignored                       | Finding                                                                                                                                          |
| -------------------------- | ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| Race-date list             | `jockeyNames`                                           | Used for option construction, selected-jockey filtering, search text, and visible race-card labels.                                              |
| Training table             | `currentJockeyName`, `trainingRiderName`, `trainerName` | All three are rendered. Premium merge preserves the rider override.                                                                              |
| Premium comments/reviews   | evaluation text, grade, rider and comment fields        | Merged and consumed by the training/paddock presentation paths.                                                                                  |
| Race trend                 | current JV jockey/trainer names                         | Intentionally retained as aggregation keys. Replacing them with display names would break historical matching.                                   |
| Pace prediction            | current JV jockey/trainer names                         | Intentionally retained for score-row selection; only the final horse label uses the presentation helper.                                         |
| Finish-position prediction | current JV jockey/trainer names                         | Intentionally retained for historical and same-day matching; only final horse/jockey labels use presentation names.                              |
| AI data                    | raw JV and supplemental identity fields                 | Raw keys remain for compatibility. Supplemental fields are serialized and the prompt/output layer prefers presentation values.                   |
| Realtime race data         | realtime jockey and horse labels                        | Horse labels use the current-runner presentation value. Jockey comparison uses JV normalization while same-identity display keeps the full name. |

## Query fields not rendered directly

A field being returned but not rendered is not by itself a defect. The following groups are deliberate or require a separate product decision rather than a mechanical UI substitution:

- `latestSource`, `detailRank`, and `umabanSort` are intermediate query/mapping fields.
- `fastest*` fields are transformed into nested detail objects before reaching the UI.
- training `7F`-`10F` totals/laps are available from the query, while the compact training table intentionally presents the standard `6F`-`1F` window.
- ability-test `ijoKubunCode` and `chakusaCode1`-`chakusaCode3` are available but the already-wide ability-test table does not currently expose abnormal-result/margin columns. Adding them needs code-label semantics and a layout decision; there is no competing stale value currently shown.
- deprecated `RaceTrendPayload.jockeyRows`/`frameRows` are retained only for cache compatibility and are intentionally omitted from new responses.

## Guardrail for future changes

Before replacing a query field in a component or serializer:

1. identify whether the field is a presentation value or a compute/join key;
2. preserve the raw key through matching and scoring;
3. substitute the presentation value only in the final output object;
4. keep existing API keys and add richer fields instead of changing their meaning;
5. test both a richer-value row and a fallback-only row.
