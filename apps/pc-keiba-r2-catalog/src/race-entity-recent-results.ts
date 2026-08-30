// Run with bun (bunx vitest). Builds bounded keyset queries over the R2 Iceberg catalog.

import type {
  CatalogSource,
  R2SqlCatalogConfig,
  RaceEntityRecentResultsFilters,
  RaceEntityType,
} from "./types";

export interface RaceEntityWarmRace {
  date: string;
  keibajoCode: string;
  raceBango: string;
  source: CatalogSource;
}

export interface RaceEntityWarmTarget {
  horseBucket: string | null;
  horseId: string | null;
  horseName: string | null;
  horseNumber: string | null;
  jockeyBucket: string | null;
  jockeyId: string | null;
  jockeyName: string | null;
  ownerBucket: string | null;
  ownerId: string | null;
  ownerName: string | null;
  raceName: string | null;
  raceStartTime: string | null;
  trainerBucket: string | null;
  trainerId: string | null;
  trainerName: string | null;
}

export interface RaceEntityTarget {
  entityBucket: string | null;
  entityId: string | null;
  entityName: string | null;
  horseId: string | null;
  horseName: string | null;
  raceName: string | null;
  raceStartTime: string | null;
  runnerFound: boolean;
}

export interface RaceEntityCursorKey {
  raceStartSortKey: string;
  resultId: string;
}

export interface RaceEntityHistoryRow extends RaceEntityCursorKey {
  abnormalityCode: string | null;
  carriedWeight: number | null;
  className: string | null;
  cornerPositions: string[];
  distance: number | null;
  fieldSize: number | null;
  final3FSeconds: number | null;
  finishPosition: number | null;
  frameNumber: string | null;
  gradeCode: string | null;
  horseId: string | null;
  horseName: string | null;
  horseNumber: string | null;
  horseWeight: number | null;
  horseWeightDiff: number | null;
  jockeyId: string | null;
  jockeyName: string | null;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  margin: string | null;
  ownerId: string | null;
  ownerName: string | null;
  popularity: number | null;
  raceId: string;
  raceName: string | null;
  raceNumber: string;
  raceStartTime: string | null;
  raceTimeSeconds: number | null;
  source: CatalogSource;
  trackCode: string | null;
  trainerId: string | null;
  trainerName: string | null;
  turfConditionCode: string | null;
  dirtConditionCode: string | null;
  weatherCode: string | null;
  winOdds: number | null;
}

interface CursorPayload extends RaceEntityCursorKey {
  fingerprint: string;
  version: number;
}

interface SignedCursorEnvelope {
  payload: string;
  signature: string;
}

interface HistoryTableSet {
  raceTable: "jvd_ra" | "nvd_ra";
  runnerTable: "jvd_se" | "nvd_se";
  source: CatalogSource;
}

interface HistorySelectInput {
  cursor: RaceEntityCursorKey | null;
  entityBuckets: readonly string[];
  entityIds: readonly string[];
  env: R2SqlCatalogConfig;
  filters: RaceEntityRecentResultsFilters;
  targetStartTime: string | null;
}

export interface RaceEntityPublicResult {
  carriedWeight: number | null;
  class: string | null;
  cornerPositions: string[];
  distance: number | null;
  fieldSize: number | null;
  final3FSeconds: number | null;
  finishPosition: number | null;
  frameNumber: string | null;
  grade: string | null;
  horseId: string | null;
  horseName: string | null;
  horseNumber: string | null;
  horseWeight: number | null;
  horseWeightDiff: number | null;
  jockeyId: string | null;
  jockeyName: string | null;
  margin: string | null;
  ownerId: string | null;
  ownerName: string | null;
  popularity: number | null;
  raceDate: string;
  raceId: string;
  raceName: string | null;
  raceNumber: string;
  raceStartAt: string | null;
  raceTimeSeconds: number | null;
  resultStatus: string;
  source: CatalogSource;
  surface: string | null;
  trackCondition: string | null;
  trackConditionCode: string | null;
  trainerId: string | null;
  trainerName: string | null;
  venue: string;
  venueCode: string;
  weather: string | null;
  weatherCode: string | null;
  winOdds: number | null;
}

export interface RaceEntityPage {
  entity: {
    entityId: string;
    entityName: string | null;
    entityType: RaceEntityType;
    horseId: string | null;
    horseName: string | null;
    horseNumber: string;
  };
  pagination: {
    effectiveLimit: number;
    hasMore: boolean;
    nextCursor: string | null;
    requestedLimit: number;
    returned: number;
  };
  results: RaceEntityPublicResult[];
  targetRace: {
    raceId: string;
    raceName: string | null;
    raceStartAt: string | null;
    source: CatalogSource;
  };
}

const IDENTIFIER_PATTERN: RegExp = /^[A-Za-z_][A-Za-z0-9_]*$/u;
const ENTITY_ID_PATTERN: RegExp = /^[A-Za-z0-9]+$/u;
const ENTITY_BUCKET_PATTERN: RegExp = /^[0-9a-f]$/u;
const SORT_KEY_PATTERN: RegExp = /^\d{12}$/u;
const RESULT_ID_PATTERN: RegExp = /^(?:jra|nar):\d{8}:[0-9A-Z]{2}:\d{2}:[^:]*:[A-Za-z0-9]*$/u;
const VALID_TIME_PATTERN: RegExp = /^[0-2][0-9][0-5][0-9]$/u;
const CURSOR_VERSION: number = 2;
const HMAC_ALGORITHM: { hash: string; name: string } = { hash: "SHA-256", name: "HMAC" };
const MIN_CURSOR_SECRET_LENGTH: number = 32;
const ENTITY_HISTORY_TABLE: string = "race_entity_history_v1";
const HORSE_MINIMUM_RACING_AGE_YEARS: number = 2;
const HORSE_FALLBACK_HISTORY_YEARS: number = 10;
const TRACK_CONDITIONS: ReadonlyMap<string, string> = new Map([
  ["0", "未設定"],
  ["1", "良"],
  ["2", "稍重"],
  ["3", "重"],
  ["4", "不良"],
]);
const WEATHER: ReadonlyMap<string, string> = new Map([
  ["0", "未設定"],
  ["1", "晴"],
  ["2", "曇"],
  ["3", "雨"],
  ["4", "小雨"],
  ["5", "雪"],
  ["6", "小雪"],
]);
const VENUES: ReadonlyMap<string, string> = new Map([
  ["01", "札幌"],
  ["02", "函館"],
  ["03", "福島"],
  ["04", "新潟"],
  ["05", "東京"],
  ["06", "中山"],
  ["07", "中京"],
  ["08", "京都"],
  ["09", "阪神"],
  ["10", "小倉"],
  ["30", "門別"],
  ["33", "帯広"],
  ["35", "盛岡"],
  ["36", "水沢"],
  ["42", "浦和"],
  ["43", "船橋"],
  ["44", "大井"],
  ["45", "川崎"],
  ["46", "金沢"],
  ["47", "笠松"],
  ["48", "名古屋"],
  ["50", "園田"],
  ["51", "姫路"],
  ["54", "高知"],
  ["55", "佐賀"],
  ["83", "帯広（ばんえい）"],
]);
const MAX_RESPONSE_BYTES: number = 64 * 1024;
const CURSOR_FILTER: string = "completed-before-target-source-scoped-index-v3";
const CURSOR_SORT: string = "raceStartSortKey DESC,resultId DESC";
const JRA_TABLES: HistoryTableSet = {
  raceTable: "jvd_ra",
  runnerTable: "jvd_se",
  source: "jra",
};
const NAR_TABLES: HistoryTableSet = {
  raceTable: "nvd_ra",
  runnerTable: "nvd_se",
  source: "nar",
};
const ENTITY_ID_COLUMNS: ReadonlyMap<RaceEntityType, string> = new Map([
  ["horse", "ketto_toroku_bango"],
  ["jockey", "kishu_code"],
  ["trainer", "chokyoshi_code"],
  ["owner", "banushi_code"],
]);
const ENTITY_NAME_COLUMNS: ReadonlyMap<RaceEntityType, string> = new Map([
  ["horse", "bamei"],
  ["jockey", "kishumei_ryakusho"],
  ["trainer", "chokyoshimei_ryakusho"],
  ["owner", "banushimei"],
]);

const requiredMapValue = (
  values: ReadonlyMap<RaceEntityType, string>,
  entityType: RaceEntityType,
): string => {
  const value = values.get(entityType);
  if (value === undefined) throw new Error("entityType is unsupported");
  return value;
};

const tableName = (env: R2SqlCatalogConfig, table: string): string => {
  if (!IDENTIFIER_PATTERN.test(env.R2_SQL_NAMESPACE)) {
    throw new Error("R2_SQL_NAMESPACE must be an unquoted SQL identifier");
  }
  return `${env.R2_SQL_NAMESPACE}.${table}`;
};

const currentTables = (source: CatalogSource): HistoryTableSet =>
  source === "jra" ? JRA_TABLES : NAR_TABLES;

const trimSql = (expression: string): string =>
  `nullif(btrim(replace(coalesce(${expression}, ''), chr(12288), '')), '')`;

const sqlLiteral = (value: string): string => `'${value.replaceAll("'", "''")}'`;

const historyLowerYear = (filters: RaceEntityRecentResultsFilters, entityId: string): string => {
  const targetYear = Number(filters.date.slice(0, 4));
  const fallback = targetYear - HORSE_FALLBACK_HISTORY_YEARS + 1;
  const birthYear = Number(entityId.slice(0, 4));
  const earliestHorseYear =
    filters.entityType === "horse" && Number.isInteger(birthYear) && birthYear <= targetYear
      ? birthYear + HORSE_MINIMUM_RACING_AGE_YEARS
      : fallback;
  return filters.entityType === "horse" ? String(Math.max(1986, earliestHorseYear)) : "1986";
};

const historyUpperYear = (
  filters: RaceEntityRecentResultsFilters,
  cursor: RaceEntityCursorKey | null,
): string => cursor?.raceStartSortKey.slice(0, 4) ?? filters.date.slice(0, 4);

export const buildRaceEntityWarmTargetsQuery = (
  env: R2SqlCatalogConfig,
  race: RaceEntityWarmRace,
): string => {
  const tables = currentTables(race.source);
  return `SELECT
  ${trimSql("se.umaban")} AS horse_number,
  substr(md5(${trimSql("se.ketto_toroku_bango")}), 1, 1) AS horse_bucket,
  ${trimSql("se.ketto_toroku_bango")} AS horse_id,
  ${trimSql("se.bamei")} AS horse_name,
  substr(md5(${trimSql("se.kishu_code")}), 1, 1) AS jockey_bucket,
  ${trimSql("se.kishu_code")} AS jockey_id,
  ${trimSql("se.kishumei_ryakusho")} AS jockey_name,
  substr(md5(${trimSql("se.chokyoshi_code")}), 1, 1) AS trainer_bucket,
  ${trimSql("se.chokyoshi_code")} AS trainer_id,
  ${trimSql("se.chokyoshimei_ryakusho")} AS trainer_name,
  substr(md5(${trimSql("se.banushi_code")}), 1, 1) AS owner_bucket,
  ${trimSql("se.banushi_code")} AS owner_id,
  ${trimSql("se.banushimei")} AS owner_name,
  ${trimSql("ra.kyosomei_hondai")} AS race_name,
  ${trimSql("ra.hasso_jikoku")} AS race_start_time
FROM ${tableName(env, tables.raceTable)} ra
INNER JOIN ${tableName(env, tables.runnerTable)} se
  ON se.kaisai_nen = ra.kaisai_nen
  AND se.kaisai_tsukihi = ra.kaisai_tsukihi
  AND se.keibajo_code = ra.keibajo_code
  AND se.race_bango = ra.race_bango
WHERE ra.kaisai_nen = ${sqlLiteral(race.date.slice(0, 4))}
  AND ra.kaisai_tsukihi = ${sqlLiteral(race.date.slice(4))}
  AND ra.keibajo_code = ${sqlLiteral(race.keibajoCode)}
  AND ra.race_bango = ${sqlLiteral(race.raceBango)}
ORDER BY try_cast(${trimSql("se.umaban")} AS INT)`;
};

export const buildRaceEntityTargetQuery = (
  env: R2SqlCatalogConfig,
  filters: RaceEntityRecentResultsFilters,
): string => {
  const tables = currentTables(filters.source);
  const entityIdColumn = requiredMapValue(ENTITY_ID_COLUMNS, filters.entityType);
  const entityNameColumn = requiredMapValue(ENTITY_NAME_COLUMNS, filters.entityType);
  return `SELECT
  substr(md5(${trimSql(`se.${entityIdColumn}`)}), 1, 1) AS entity_bucket,
  ${trimSql(`se.${entityIdColumn}`)} AS entity_id,
  ${trimSql(`se.${entityNameColumn}`)} AS entity_name,
  ${trimSql("se.ketto_toroku_bango")} AS horse_id,
  ${trimSql("se.bamei")} AS horse_name,
  ${trimSql("ra.kyosomei_hondai")} AS race_name,
  ${trimSql("ra.hasso_jikoku")} AS race_start_time,
  CASE WHEN se.umaban IS NULL THEN false ELSE true END AS runner_found
FROM ${tableName(env, tables.raceTable)} ra
LEFT JOIN ${tableName(env, tables.runnerTable)} se
  ON se.kaisai_nen = ra.kaisai_nen
  AND se.kaisai_tsukihi = ra.kaisai_tsukihi
  AND se.keibajo_code = ra.keibajo_code
  AND se.race_bango = ra.race_bango
  AND try_cast(${trimSql("se.umaban")} AS INT) = ${String(Number(filters.horseNumber))}
WHERE ra.kaisai_nen = ${sqlLiteral(filters.date.slice(0, 4))}
  AND ra.kaisai_tsukihi = ${sqlLiteral(filters.date.slice(4))}
  AND ra.keibajo_code = ${sqlLiteral(filters.keibajoCode)}
  AND ra.race_bango = ${sqlLiteral(filters.raceBango)}
LIMIT 1`;
};

const stringOrNull = (value: unknown): string | null => {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") return value;
  return typeof value === "number" || typeof value === "bigint" || typeof value === "boolean"
    ? String(value)
    : null;
};

const booleanValue = (value: unknown): boolean => value === true || value === "true" || value === 1;
const numberOrNull = (value: unknown): number | null => {
  const parsed = Number(value);
  return value === null || value === undefined || !Number.isFinite(parsed) ? null : parsed;
};

export const normaliseRaceEntityWarmTarget = (
  row: Record<string, unknown>,
): RaceEntityWarmTarget => ({
  horseBucket: stringOrNull(row.horse_bucket),
  horseId: stringOrNull(row.horse_id),
  horseName: stringOrNull(row.horse_name),
  horseNumber: stringOrNull(row.horse_number),
  jockeyBucket: stringOrNull(row.jockey_bucket),
  jockeyId: stringOrNull(row.jockey_id),
  jockeyName: stringOrNull(row.jockey_name),
  ownerBucket: stringOrNull(row.owner_bucket),
  ownerId: stringOrNull(row.owner_id),
  ownerName: stringOrNull(row.owner_name),
  raceName: stringOrNull(row.race_name),
  raceStartTime: stringOrNull(row.race_start_time),
  trainerBucket: stringOrNull(row.trainer_bucket),
  trainerId: stringOrNull(row.trainer_id),
  trainerName: stringOrNull(row.trainer_name),
});

export const normaliseRaceEntityTarget = (row: Record<string, unknown>): RaceEntityTarget => ({
  entityBucket: stringOrNull(row.entity_bucket),
  entityId: stringOrNull(row.entity_id),
  entityName: stringOrNull(row.entity_name),
  horseId: stringOrNull(row.horse_id),
  horseName: stringOrNull(row.horse_name),
  raceName: stringOrNull(row.race_name),
  raceStartTime: stringOrNull(row.race_start_time),
  runnerFound: booleanValue(row.runner_found),
});

const historySelect = ({
  cursor,
  entityBuckets,
  entityIds,
  env,
  filters,
  targetStartTime,
}: HistorySelectInput): string => {
  const targetYear = filters.date.slice(0, 4);
  const targetMonthDay = filters.date.slice(4);
  const lowerYear = Math.min(
    ...entityIds.map((entityId) => Number(historyLowerYear(filters, entityId))),
  );
  const upperYear = historyUpperYear(filters, cursor);
  const entityIdsSql = entityIds.map(sqlLiteral).join(", ");
  const entityBucketsSql = entityBuckets.map(sqlLiteral).join(", ");
  const lowerYearSql = sqlLiteral(String(lowerYear));
  const startTimeSql = sqlLiteral(targetStartTime ?? "");
  const startSortKey = `concat(se.kaisai_nen, se.kaisai_tsukihi, coalesce(${trimSql("se.hasso_jikoku")}, '0000'))`;
  const resultId = "se.result_id";
  const sourcePredicate =
    filters.entityType === "horse"
      ? "se.source IN ('jra', 'nar')"
      : `se.source = ${sqlLiteral(filters.source)}`;
  const cursorPredicate =
    cursor === null
      ? "true"
      : `(${startSortKey} < ${sqlLiteral(cursor.raceStartSortKey)} OR (${startSortKey} = ${sqlLiteral(cursor.raceStartSortKey)} AND ${resultId} < ${sqlLiteral(cursor.resultId)}))`;
  const sameDateBefore = `(
      se.kaisai_nen = ${sqlLiteral(targetYear)}
      AND se.kaisai_tsukihi = ${sqlLiteral(targetMonthDay)}
      AND (
        (
          regexp_match(${startTimeSql}, '^[0-2][0-9][0-5][0-9]$') IS NOT NULL
          AND regexp_match(${trimSql("se.hasso_jikoku")}, '^[0-2][0-9][0-5][0-9]$') IS NOT NULL
          AND ${trimSql("se.hasso_jikoku")} < ${startTimeSql}
        )
        OR (
          se.source = ${sqlLiteral(filters.source)}
          AND se.keibajo_code = ${sqlLiteral(filters.keibajoCode)}
          AND se.race_bango < ${sqlLiteral(filters.raceBango)}
          AND (
            regexp_match(${startTimeSql}, '^[0-2][0-9][0-5][0-9]$') IS NULL
            OR regexp_match(${trimSql("se.hasso_jikoku")}, '^[0-2][0-9][0-5][0-9]$') IS NULL
          )
        )
      )
      AND (
        coalesce(${trimSql("se.kakutei_chakujun")}, '00') <> '00'
        OR coalesce(${trimSql("se.ijo_kubun_code")}, '0') <> '0'
      )
    )`;
  return `SELECT
  se.entity_id AS matched_entity_id,
  se.source,
  ${startSortKey} AS race_start_sort_key,
  concat(se.source, ':', se.kaisai_nen, se.kaisai_tsukihi, ':', se.keibajo_code, ':', se.race_bango) AS race_id,
  ${resultId} AS result_id,
  se.kaisai_nen,
  se.kaisai_tsukihi,
  se.keibajo_code,
  se.race_bango,
  ${trimSql("se.hasso_jikoku")} AS race_start_time,
  ${trimSql("se.kyosomei_hondai")} AS race_name,
  ${trimSql("se.kyoso_joken_meisho")} AS class_name,
  ${trimSql("se.grade_code")} AS grade_code,
  try_cast(${trimSql("se.kyori")} AS INT) AS distance,
  ${trimSql("se.track_code")} AS track_code,
  ${trimSql("se.tenko_code")} AS weather_code,
  ${trimSql("se.babajotai_code_shiba")} AS turf_condition_code,
  ${trimSql("se.babajotai_code_dirt")} AS dirt_condition_code,
  try_cast(${trimSql("se.shusso_tosu")} AS INT) AS field_size,
  ${trimSql("se.ketto_toroku_bango")} AS horse_id,
  ${trimSql("se.bamei")} AS horse_name,
  ${trimSql("se.kishu_code")} AS jockey_id,
  ${trimSql("se.kishumei_ryakusho")} AS jockey_name,
  ${trimSql("se.chokyoshi_code")} AS trainer_id,
  ${trimSql("se.chokyoshimei_ryakusho")} AS trainer_name,
  ${trimSql("se.banushi_code")} AS owner_id,
  ${trimSql("se.banushimei")} AS owner_name,
  try_cast(nullif(${trimSql("se.kakutei_chakujun")}, '00') AS INT) AS finish_position,
  ${trimSql("se.ijo_kubun_code")} AS abnormality_code,
  try_cast(nullif(${trimSql("se.tansho_ninkijun")}, '00') AS INT) AS popularity,
  try_cast(nullif(${trimSql("se.tansho_odds")}, '0000') AS FLOAT) / 10.0 AS win_odds,
  try_cast(nullif(${trimSql("se.soha_time")}, '0000') AS FLOAT) / 10.0 AS race_time_seconds,
  ${trimSql("se.time_sa")} AS margin,
  try_cast(nullif(${trimSql("se.kohan_3f")}, '000') AS FLOAT) / 10.0 AS final_3f_seconds,
  ${trimSql("se.corner_1")} AS corner_1,
  ${trimSql("se.corner_2")} AS corner_2,
  ${trimSql("se.corner_3")} AS corner_3,
  ${trimSql("se.corner_4")} AS corner_4,
  ${trimSql("se.umaban")} AS horse_number,
  ${trimSql("se.wakuban")} AS frame_number,
  try_cast(nullif(${trimSql("se.futan_juryo")}, '000') AS FLOAT) / 10.0 AS carried_weight,
  try_cast(nullif(${trimSql("se.bataiju")}, '000') AS INT) AS horse_weight,
  CASE
    WHEN ${trimSql("se.zogen_sa")} IS NULL OR ${trimSql("se.zogen_sa")} = '000' THEN NULL
    WHEN ${trimSql("se.zogen_fugo")} = '-' THEN -try_cast(${trimSql("se.zogen_sa")} AS INT)
    ELSE try_cast(${trimSql("se.zogen_sa")} AS INT)
  END AS horse_weight_diff
FROM ${tableName(env, ENTITY_HISTORY_TABLE)} se
WHERE se.entity_type = ${sqlLiteral(filters.entityType)}
  AND ${sourcePredicate}
  AND se.entity_bucket IN (${entityBucketsSql})
  AND se.kaisai_nen >= ${lowerYearSql}
  AND se.kaisai_nen <= ${sqlLiteral(upperYear)}
  AND se.entity_id IN (${entityIdsSql})
  AND ${cursorPredicate}
  AND (
    se.kaisai_nen < ${sqlLiteral(targetYear)}
    OR (
      se.kaisai_nen = ${sqlLiteral(targetYear)}
      AND se.kaisai_tsukihi < ${sqlLiteral(targetMonthDay)}
    )
    OR ${sameDateBefore}
  )`;
};

export const buildRaceEntityHistoryQuery = (
  env: R2SqlCatalogConfig,
  filters: RaceEntityRecentResultsFilters,
  target: RaceEntityTarget & { entityBucket: string; entityId: string },
  cursor: RaceEntityCursorKey | null,
): string => {
  if (!ENTITY_ID_PATTERN.test(target.entityId)) throw new Error("entityId is malformed");
  if (!ENTITY_BUCKET_PATTERN.test(target.entityBucket))
    throw new Error("entityBucket is malformed");
  const select = historySelect({
    cursor,
    entityBuckets: [target.entityBucket],
    entityIds: [target.entityId],
    env,
    filters,
    targetStartTime: target.raceStartTime,
  });
  return `WITH history AS (
${select}
)
SELECT * FROM history
ORDER BY race_start_sort_key DESC, result_id DESC
LIMIT ${String(filters.limit + 1)}`;
};

export const buildRaceEntityWarmHistoryQuery = (
  env: R2SqlCatalogConfig,
  filters: RaceEntityRecentResultsFilters,
  targets: readonly (RaceEntityTarget & { entityBucket: string; entityId: string })[],
): string => {
  const first = targets[0];
  if (first === undefined) throw new Error("warm targets are required");
  const entityIds = targets.map((target) => target.entityId);
  const entityBuckets = targets.map((target) => target.entityBucket);
  if (!entityIds.every((entityId) => ENTITY_ID_PATTERN.test(entityId))) {
    throw new Error("entityId is malformed");
  }
  if (!entityBuckets.every((entityBucket) => ENTITY_BUCKET_PATTERN.test(entityBucket))) {
    throw new Error("entityBucket is malformed");
  }
  const select = historySelect({
    cursor: null,
    entityBuckets,
    entityIds,
    env,
    filters,
    targetStartTime: first.raceStartTime,
  });
  return `WITH history AS (
${select}
)
SELECT * FROM history
QUALIFY row_number() OVER (
  PARTITION BY matched_entity_id
  ORDER BY race_start_sort_key DESC, result_id DESC
) <= ${String(filters.limit + 1)}
ORDER BY matched_entity_id, race_start_sort_key DESC, result_id DESC`;
};

const requiredString = (row: Record<string, unknown>, field: string): string => {
  const value = stringOrNull(row[field]);
  if (value === null || value.length === 0) throw new Error(`R2 SQL row is missing ${field}`);
  return value;
};
const cornerPositions = (row: Record<string, unknown>): string[] =>
  [row.corner_1, row.corner_2, row.corner_3, row.corner_4]
    .map(stringOrNull)
    .filter((value): value is string => value !== null);

export const normaliseRaceEntityHistoryRow = (
  row: Record<string, unknown>,
): RaceEntityHistoryRow => ({
  abnormalityCode: stringOrNull(row.abnormality_code),
  carriedWeight: numberOrNull(row.carried_weight),
  className: stringOrNull(row.class_name),
  cornerPositions: cornerPositions(row),
  dirtConditionCode: stringOrNull(row.dirt_condition_code),
  distance: numberOrNull(row.distance),
  fieldSize: numberOrNull(row.field_size),
  final3FSeconds: numberOrNull(row.final_3f_seconds),
  finishPosition: numberOrNull(row.finish_position),
  frameNumber: stringOrNull(row.frame_number),
  gradeCode: stringOrNull(row.grade_code),
  horseId: stringOrNull(row.horse_id),
  horseName: stringOrNull(row.horse_name),
  horseNumber: stringOrNull(row.horse_number),
  horseWeight: numberOrNull(row.horse_weight),
  horseWeightDiff: numberOrNull(row.horse_weight_diff),
  jockeyId: stringOrNull(row.jockey_id),
  jockeyName: stringOrNull(row.jockey_name),
  kaisaiNen: requiredString(row, "kaisai_nen"),
  kaisaiTsukihi: requiredString(row, "kaisai_tsukihi"),
  keibajoCode: requiredString(row, "keibajo_code"),
  margin: stringOrNull(row.margin),
  ownerId: stringOrNull(row.owner_id),
  ownerName: stringOrNull(row.owner_name),
  popularity: numberOrNull(row.popularity),
  raceId: requiredString(row, "race_id"),
  raceName: stringOrNull(row.race_name),
  raceNumber: requiredString(row, "race_bango"),
  raceStartSortKey: requiredString(row, "race_start_sort_key"),
  raceStartTime: stringOrNull(row.race_start_time),
  raceTimeSeconds: numberOrNull(row.race_time_seconds),
  resultId: requiredString(row, "result_id"),
  source: requiredString(row, "source") === "jra" ? "jra" : "nar",
  trackCode: stringOrNull(row.track_code),
  trainerId: stringOrNull(row.trainer_id),
  trainerName: stringOrNull(row.trainer_name),
  turfConditionCode: stringOrNull(row.turf_condition_code),
  weatherCode: stringOrNull(row.weather_code),
  winOdds: numberOrNull(row.win_odds),
});

const fingerprintInput = (filters: RaceEntityRecentResultsFilters, entityId: string): string =>
  JSON.stringify({
    entityId,
    entityType: filters.entityType,
    filter: CURSOR_FILTER,
    sort: CURSOR_SORT,
    targetRace: `${filters.source}:${filters.date}:${filters.keibajoCode}:${filters.raceBango}`,
  });

const fingerprint = async (
  filters: RaceEntityRecentResultsFilters,
  entityId: string,
): Promise<string> => {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(fingerprintInput(filters, entityId)),
  );
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
};

const base64UrlEncode = (value: Uint8Array): string =>
  btoa(String.fromCharCode(...value))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replace(/=+$/u, "");

const base64UrlDecode = (value: string): Uint8Array => {
  const base64 = value.replaceAll("-", "+").replaceAll("_", "/");
  const padded = base64.padEnd(Math.ceil(base64.length / 4) * 4, "=");
  return Uint8Array.from(atob(padded), (character) => character.charCodeAt(0));
};

const encodeText = (value: string): Uint8Array => new TextEncoder().encode(value);
const decodeText = (value: Uint8Array): string => new TextDecoder().decode(value);

const cursorSigningKey = (secret: string): Promise<CryptoKey> => {
  if (secret.length < MIN_CURSOR_SECRET_LENGTH) {
    throw new Error("RACE_ENTITY_CURSOR_SECRET must contain at least 32 characters");
  }
  return crypto.subtle.importKey("raw", encodeText(secret), HMAC_ALGORITHM, false, [
    "sign",
    "verify",
  ]);
};

const isCursorPayload = (value: unknown): value is CursorPayload => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const entries = Object.fromEntries(Object.entries(value));
  return (
    entries.version === CURSOR_VERSION &&
    typeof entries.fingerprint === "string" &&
    typeof entries.raceStartSortKey === "string" &&
    SORT_KEY_PATTERN.test(entries.raceStartSortKey) &&
    typeof entries.resultId === "string" &&
    RESULT_ID_PATTERN.test(entries.resultId)
  );
};

const isSignedCursorEnvelope = (value: unknown): value is SignedCursorEnvelope => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const entries = Object.fromEntries(Object.entries(value));
  return typeof entries.payload === "string" && typeof entries.signature === "string";
};

export const createRaceEntityCursor = async (
  filters: RaceEntityRecentResultsFilters,
  entityId: string,
  key: RaceEntityCursorKey,
  secret: string,
): Promise<string> => {
  const payload = base64UrlEncode(
    encodeText(
      JSON.stringify({
        fingerprint: await fingerprint(filters, entityId),
        raceStartSortKey: key.raceStartSortKey,
        resultId: key.resultId,
        version: CURSOR_VERSION,
      } satisfies CursorPayload),
    ),
  );
  const signature = await crypto.subtle.sign(
    HMAC_ALGORITHM,
    await cursorSigningKey(secret),
    encodeText(payload),
  );
  return base64UrlEncode(
    encodeText(
      JSON.stringify({
        payload,
        signature: base64UrlEncode(new Uint8Array(signature)),
      } satisfies SignedCursorEnvelope),
    ),
  );
};

export const parseRaceEntityCursor = async (
  filters: RaceEntityRecentResultsFilters,
  entityId: string,
  secret: string,
): Promise<RaceEntityCursorKey | null | "invalid"> => {
  if (filters.cursor === null) return null;
  try {
    const envelopeValue: unknown = JSON.parse(decodeText(base64UrlDecode(filters.cursor)));
    if (!isSignedCursorEnvelope(envelopeValue)) return "invalid";
    const validSignature = await crypto.subtle.verify(
      HMAC_ALGORITHM,
      await cursorSigningKey(secret),
      base64UrlDecode(envelopeValue.signature),
      encodeText(envelopeValue.payload),
    );
    if (!validSignature) return "invalid";
    const payloadValue: unknown = JSON.parse(decodeText(base64UrlDecode(envelopeValue.payload)));
    if (!isCursorPayload(payloadValue)) return "invalid";
    return payloadValue.fingerprint === (await fingerprint(filters, entityId))
      ? { raceStartSortKey: payloadValue.raceStartSortKey, resultId: payloadValue.resultId }
      : "invalid";
  } catch {
    return "invalid";
  }
};

const raceStartAt = (date: string, time: string | null): string | null =>
  time === null || !VALID_TIME_PATTERN.test(time)
    ? null
    : `${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}T${time.slice(0, 2)}:${time.slice(2, 4)}:00+09:00`;

const surface = (trackCode: string | null): string | null => {
  if (trackCode === null) return null;
  const numeric = Number(trackCode);
  if (numeric >= 10 && numeric <= 22) return "芝";
  if (numeric >= 23 && numeric <= 29) return numeric >= 27 && numeric <= 28 ? "サンド" : "ダート";
  return numeric >= 51 && numeric <= 59 ? "障害" : null;
};

const resultStatus = (row: RaceEntityHistoryRow): string => {
  if (row.finishPosition !== null) return "finished";
  return row.abnormalityCode === null ? "unknown" : `abnormal:${row.abnormalityCode}`;
};

const trackConditionCode = (row: RaceEntityHistoryRow): string | null =>
  surface(row.trackCode) === "芝" ? row.turfConditionCode : row.dirtConditionCode;

const labelFor = (labels: ReadonlyMap<string, string>, code: string | null): string | null =>
  code === null ? null : (labels.get(code) ?? null);

const publicResult = (row: RaceEntityHistoryRow): RaceEntityPublicResult => ({
  carriedWeight: row.carriedWeight,
  class: row.className,
  cornerPositions: row.cornerPositions,
  distance: row.distance,
  fieldSize: row.fieldSize,
  final3FSeconds: row.final3FSeconds,
  finishPosition: row.finishPosition,
  frameNumber: row.frameNumber,
  grade: row.gradeCode,
  horseId: row.horseId,
  horseName: row.horseName,
  horseNumber: row.horseNumber,
  horseWeight: row.horseWeight,
  horseWeightDiff: row.horseWeightDiff,
  jockeyId: row.jockeyId,
  jockeyName: row.jockeyName,
  margin: row.margin,
  ownerId: row.ownerId,
  ownerName: row.ownerName,
  popularity: row.popularity,
  raceDate: `${row.kaisaiNen}-${row.kaisaiTsukihi.slice(0, 2)}-${row.kaisaiTsukihi.slice(2)}`,
  raceId: row.raceId,
  raceName: row.raceName,
  raceNumber: row.raceNumber,
  raceStartAt: raceStartAt(`${row.kaisaiNen}${row.kaisaiTsukihi}`, row.raceStartTime),
  raceTimeSeconds: row.raceTimeSeconds,
  resultStatus: resultStatus(row),
  source: row.source,
  surface: surface(row.trackCode),
  trackCondition: labelFor(TRACK_CONDITIONS, trackConditionCode(row)),
  trackConditionCode: trackConditionCode(row),
  trainerId: row.trainerId,
  trainerName: row.trainerName,
  venue: VENUES.get(row.keibajoCode) ?? row.keibajoCode,
  venueCode: row.keibajoCode,
  weather: labelFor(WEATHER, row.weatherCode),
  weatherCode: row.weatherCode,
  winOdds: row.winOdds,
});

const pageWithRowCount = async (
  filters: RaceEntityRecentResultsFilters,
  target: RaceEntityTarget & { entityId: string },
  rows: RaceEntityHistoryRow[],
  rowCount: number,
  secret: string,
): Promise<RaceEntityPage> => {
  const selected = rows.slice(0, rowCount);
  const hasMore = rows.length > selected.length;
  const last = selected.at(-1);
  const nextCursor =
    hasMore && last !== undefined
      ? await createRaceEntityCursor(filters, target.entityId, last, secret)
      : null;
  return {
    entity: {
      entityId: target.entityId,
      entityName: target.entityName,
      entityType: filters.entityType,
      horseId: target.horseId,
      horseName: target.horseName,
      horseNumber: filters.horseNumber.padStart(2, "0"),
    },
    pagination: {
      effectiveLimit: rowCount,
      hasMore,
      nextCursor,
      requestedLimit: filters.limit,
      returned: selected.length,
    },
    results: selected.map(publicResult),
    targetRace: {
      raceId: `${filters.source}:${filters.date}:${filters.keibajoCode}:${filters.raceBango}`,
      raceName: target.raceName,
      raceStartAt: raceStartAt(filters.date, target.raceStartTime),
      source: filters.source,
    },
  };
};

const boundedPage = async (
  filters: RaceEntityRecentResultsFilters,
  target: RaceEntityTarget & { entityId: string },
  rows: RaceEntityHistoryRow[],
  rowCount: number,
  secret: string,
): Promise<RaceEntityPage> => {
  const page = await pageWithRowCount(filters, target, rows, rowCount, secret);
  const bytes = new TextEncoder().encode(JSON.stringify(page)).byteLength;
  if (bytes <= MAX_RESPONSE_BYTES) return page;
  if (rowCount <= 1) {
    throw new Error("A single race entity history row exceeds the response hard limit");
  }
  return boundedPage(filters, target, rows, rowCount - 1, secret);
};

export const buildRaceEntityPage = (
  filters: RaceEntityRecentResultsFilters,
  target: RaceEntityTarget & { entityId: string },
  rows: RaceEntityHistoryRow[],
  secret: string,
): Promise<RaceEntityPage> => boundedPage(filters, target, rows, filters.limit, secret);
