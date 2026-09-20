// Run with bun (bunx vitest).
import {
  partnershipStartDate,
  partnershipSurface,
  type PartnershipKind,
  type PartnershipScope,
} from "./heatmap-partnership";
import type { CatalogSource, R2SqlCatalogConfig } from "./types";
import {
  currentTables,
  finishPositionSql,
  tableName,
  trackSurfaceSql,
} from "./win-rate-heatmap-stats";

export interface PartnershipQueryInput {
  config: R2SqlCatalogConfig;
  scope: PartnershipScope;
  horseIds: readonly string[];
}

export interface PartnershipEntriesQueryInput {
  config: R2SqlCatalogConfig;
  scope: PartnershipScope;
  raceBango: string;
}

const RACE_NUMBER_PATTERN: RegExp = /^\d{2}$/u;
const PARTNER_COLUMNS: Record<PartnershipKind, string> = {
  horseJockey: "ketto_toroku_bango",
  jockeyTrainerVenue: "chokyoshi_code",
  jockeyVenue: "kishu_code",
  ownerVenue: "banushi_code",
  jockeyTrainerOwner: "chokyoshi_code",
};
const HORSE_ID_PATTERN: RegExp = /^\d{1,20}$/u;

// Ban-ei is a separate discipline, not flat dirt or a JRA obstacle race.
const partnershipSurfaceSql = (column: string): string =>
  `CASE WHEN btrim(${column}) = '90' THEN 'ばんえい' ELSE ${trackSurfaceSql(column)} END`;

export const buildPartnershipEntriesQuery = (input: PartnershipEntriesQueryInput): string => {
  partnershipStartDate(input.scope);
  if (!RACE_NUMBER_PATTERN.test(input.raceBango))
    throw new Error("Partnership race number must contain two digits");
  return `SELECT DISTINCT
    se.umaban, se.ketto_toroku_bango AS horse_id, se.kishu_code AS jockey_id,
    se.chokyoshi_code AS trainer_id, se.banushi_code AS owner_id, se.banushimei AS owner_name, se.bamei AS horse_name,
    ${partnershipSurfaceSql("ra.track_code")} AS surface,
    se.kishumei_ryakusho AS jockey_name, se.chokyoshimei_ryakusho AS trainer_name
  FROM ${tableName(input.config, currentTables(input.scope.source).runnerTable)} se
  INNER JOIN ${tableName(input.config, currentTables(input.scope.source).raceTable)} ra
    ON ra.kaisai_nen = se.kaisai_nen AND ra.kaisai_tsukihi = se.kaisai_tsukihi
    AND ra.keibajo_code = se.keibajo_code AND ra.race_bango = se.race_bango
  WHERE se.kaisai_nen = '${input.scope.date.slice(0, 4)}'
    AND se.kaisai_tsukihi = '${input.scope.date.slice(4)}'
    AND se.keibajo_code = '${input.scope.keibajoCode}'
    AND se.race_bango = '${input.raceBango}'
  ORDER BY se.umaban`;
};

const horsePredicate = (input: PartnershipQueryInput): string => {
  if (input.scope.kind !== "horseJockey") return "";
  if (input.horseIds.length === 0 || !input.horseIds.every((id) => HORSE_ID_PATTERN.test(id))) {
    throw new Error("Horse partnership queries require valid target horse IDs");
  }
  return `AND btrim(se.ketto_toroku_bango) IN (${[...new Set(input.horseIds)]
    .toSorted()
    .map((id) => `'${id}'`)
    .join(", ")})`;
};

// Keep thirty-year cohorts bounded to today's actual owners/combinations.
// The triple cache is shared across venues, so its target identities are too.
const ownerTargetPredicate = (input: PartnershipQueryInput): string => {
  if (input.scope.kind !== "ownerVenue" && input.scope.kind !== "jockeyTrainerOwner") return "";
  const identity: string =
    input.scope.kind === "ownerVenue"
      ? `AND upcoming.keibajo_code = '${input.scope.keibajoCode}'`
      : "AND btrim(upcoming.kishu_code) = btrim(se.kishu_code) AND btrim(upcoming.chokyoshi_code) = btrim(se.chokyoshi_code)";
  return `AND EXISTS (
    SELECT 1 FROM ${tableName(input.config, currentTables(input.scope.source).runnerTable)} upcoming
    WHERE upcoming.kaisai_nen = '${input.scope.date.slice(0, 4)}'
      AND upcoming.kaisai_tsukihi = '${input.scope.date.slice(4)}'
      AND nullif(btrim(upcoming.banushi_code), '') = nullif(btrim(se.banushi_code), '')
      ${identity}
  )`;
};

const historySources = (input: PartnershipQueryInput): readonly CatalogSource[] =>
  input.scope.kind === "jockeyTrainerOwner" ? ["jra", "nar"] : [input.scope.source];

const historyWhere = (input: PartnershipQueryInput, source: CatalogSource): string => {
  const start: string | null = partnershipStartDate(input.scope);
  const lower: string =
    start === null ? "" : `AND concat(se.kaisai_nen, se.kaisai_tsukihi) >= '${start}'`;
  const venue: string =
    input.scope.kind === "horseJockey" || input.scope.kind === "jockeyTrainerOwner"
      ? ""
      : `AND se.keibajo_code = '${input.scope.keibajoCode}'`;
  const surface: string = partnershipSurface(input.scope);
  const surfacePredicate: string =
    input.scope.kind === "horseJockey"
      ? ""
      : `AND EXISTS (
    SELECT 1 FROM ${tableName(input.config, currentTables(source).raceTable)} ra
    WHERE ra.kaisai_nen = se.kaisai_nen AND ra.kaisai_tsukihi = se.kaisai_tsukihi
      AND ra.keibajo_code = se.keibajo_code AND ra.race_bango = se.race_bango
      AND ${partnershipSurfaceSql("ra.track_code")} = '${surface}'
  )`;
  return `concat(se.kaisai_nen, se.kaisai_tsukihi) < '${input.scope.date}'\n    ${lower}\n    ${venue}\n    ${surfacePredicate}\n    ${horsePredicate(input)}\n    ${ownerTargetPredicate(input)}`;
};

// Both queries use the same cohort. Venue cohorts always match the target
// surface, independently of the optional similar-race filters.
export const buildPartnershipTargetRacesQuery = (input: PartnershipQueryInput): string => `
${historySources(input)
  .map(
    (source) => `SELECT DISTINCT
  '${source}' AS source,
  se.kaisai_nen, se.kaisai_tsukihi, se.keibajo_code, se.race_bango
FROM ${tableName(input.config, currentTables(source).runnerTable)} se
WHERE ${historyWhere(input, source)}`,
  )
  .join("\nUNION ALL\n")}
ORDER BY kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`;

export const buildPartnershipCountsQuery = (input: PartnershipQueryInput): string => `
WITH history AS (
  ${historySources(input)
    .map(
      (source) => `SELECT DISTINCT
    '${source}' AS source,
    se.kaisai_nen, se.kaisai_tsukihi, se.keibajo_code, se.race_bango, se.umaban,
    ${input.scope.kind === "ownerVenue" ? "'owner'" : "nullif(btrim(se.kishu_code), '')"} AS jockey_id,
    ${input.scope.kind === "jockeyTrainerOwner" ? "nullif(btrim(se.banushi_code), '')" : "'unused'"} AS owner_id,
    nullif(btrim(se.${PARTNER_COLUMNS[input.scope.kind]}), '') AS partner_id,
    ${finishPositionSql("se")} AS finish_position
  FROM ${tableName(input.config, currentTables(source).runnerTable)} se
  WHERE ${historyWhere(input, source)}`,
    )
    .join("\nUNION ALL\n")}
)
SELECT jockey_id, partner_id, owner_id,
  count(*) AS starts,
  sum(CASE WHEN finish_position = 1 THEN 1 ELSE 0 END) AS wins,
  sum(CASE WHEN finish_position <= 2 THEN 1 ELSE 0 END) AS places,
  sum(CASE WHEN finish_position <= 3 THEN 1 ELSE 0 END) AS shows
FROM history
WHERE finish_position > 0 AND jockey_id IS NOT NULL AND partner_id IS NOT NULL AND owner_id IS NOT NULL
GROUP BY jockey_id, partner_id, owner_id
ORDER BY jockey_id, partner_id, owner_id`;
