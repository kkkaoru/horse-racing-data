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

## Closed display-only consumers

The presentation follow-up now routes all audited current-runner labels through the shared `getRunnerDisplayNames` helper (`full -> JV -> empty`):

- `race-detail-page.tsx` processed runner rows and running-style labels;
- paddock rows and realtime odds labels;
- overall-score, premium-data-top, and time-score labels in `detail-section-data.ts`;
- pace and finish-position prediction output labels, after computation;
- similar-race, bloodline, combined-score, and newcomer/result current-runner labels.

The exported data retains the existing fixed-width JV keys for compatibility and adds supplemental fields alongside them. Realtime jockey updates compare against the JV abbreviation but preserve the full supplemental name when the realtime value identifies the same jockey. A genuinely changed jockey still replaces the stored display value.

## Consumers that must continue to use JV values for computation

Do **not** blindly replace every grep match. The following values are identity/join inputs into historical JV-derived statistics:

- `race-trend-payload.server.ts`: jockey/trainer names used to match trend aggregates;
- `race-pace-prediction.ts`: current jockey/trainer values used to select score rows;
- `finish-position-prediction.ts`: jockey/trainer equality and same-day score lookup;
- SQL/statistical functions whose historical tables contain only JV names.

Changing these to profile display names would make a full foreign name fail to match historical rows keyed by a JV abbreviation. Compute with the existing JV value, then substitute the supplemental value only in the final presentation object.

## Implemented boundary

1. The four presentation names use one tested helper (`full -> JV -> empty`).
2. The helper is called only at UI/export output boundaries.
3. Raw JV fields remain in API output; supplemental fields are additive.
4. Placeholder overseas and domestic fallback behavior is covered by helper and integration tests.
5. Score lookup, historical joins, and fixed-width JV columns remain unchanged.
