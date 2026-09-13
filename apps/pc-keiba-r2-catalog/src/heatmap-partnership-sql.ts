// Run with bun (bunx vitest).
import {
  partnershipStartDate,
  type PartnershipKind,
  type PartnershipScope,
} from "./heatmap-partnership";
import type { R2SqlCatalogConfig } from "./types";
import { currentTables, finishPositionSql, tableName } from "./win-rate-heatmap-stats";

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
};
const HORSE_ID_PATTERN: RegExp = /^\d{1,20}$/u;

export const buildPartnershipEntriesQuery = (input: PartnershipEntriesQueryInput): string => {
  partnershipStartDate(input.scope);
  if (!RACE_NUMBER_PATTERN.test(input.raceBango))
    throw new Error("Partnership race number must contain two digits");
  return `SELECT DISTINCT
    se.umaban, se.ketto_toroku_bango AS horse_id, se.kishu_code AS jockey_id,
    se.chokyoshi_code AS trainer_id, se.bamei AS horse_name,
    se.kishumei_ryakusho AS jockey_name, se.chokyoshimei_ryakusho AS trainer_name
  FROM ${tableName(input.config, currentTables(input.scope.source).runnerTable)} se
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

const historyWhere = (input: PartnershipQueryInput): string => {
  const start: string | null = partnershipStartDate(input.scope);
  const lower: string =
    start === null ? "" : `AND concat(se.kaisai_nen, se.kaisai_tsukihi) >= '${start}'`;
  const venue: string =
    input.scope.kind === "horseJockey" ? "" : `AND se.keibajo_code = '${input.scope.keibajoCode}'`;
  return `concat(se.kaisai_nen, se.kaisai_tsukihi) < '${input.scope.date}'\n    ${lower}\n    ${venue}\n    ${horsePredicate(input)}`;
};

// Both queries use exactly the same independent cohort. No distance, surface,
// grade, race title or current-race exclusions are inherited from similar races.
export const buildPartnershipTargetRacesQuery = (input: PartnershipQueryInput): string => `
SELECT DISTINCT
  '${input.scope.source}' AS source,
  se.kaisai_nen,
  se.kaisai_tsukihi,
  se.keibajo_code,
  se.race_bango
FROM ${tableName(input.config, currentTables(input.scope.source).runnerTable)} se
WHERE ${historyWhere(input)}
ORDER BY se.kaisai_nen, se.kaisai_tsukihi, se.keibajo_code, se.race_bango`;

export const buildPartnershipCountsQuery = (input: PartnershipQueryInput): string => `
WITH history AS (
  SELECT DISTINCT
    se.kaisai_nen, se.kaisai_tsukihi, se.keibajo_code, se.race_bango, se.umaban,
    nullif(btrim(se.kishu_code), '') AS jockey_id,
    nullif(btrim(se.${PARTNER_COLUMNS[input.scope.kind]}), '') AS partner_id,
    ${finishPositionSql("se")} AS finish_position
  FROM ${tableName(input.config, currentTables(input.scope.source).runnerTable)} se
  WHERE ${historyWhere(input)}
)
SELECT jockey_id, partner_id,
  count(*) AS starts,
  sum(CASE WHEN finish_position = 1 THEN 1 ELSE 0 END) AS wins,
  sum(CASE WHEN finish_position <= 2 THEN 1 ELSE 0 END) AS places,
  sum(CASE WHEN finish_position <= 3 THEN 1 ELSE 0 END) AS shows
FROM history
WHERE finish_position > 0 AND jockey_id IS NOT NULL AND partner_id IS NOT NULL
GROUP BY jockey_id, partner_id
ORDER BY jockey_id, partner_id`;
