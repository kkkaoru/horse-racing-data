// Run with bun (bunx vitest).

import type {
  CatalogSource,
  R2SqlCatalogConfig,
  WinRateHeatmapBloodlineCategory,
  WinRateHeatmapBloodlineRow,
  WinRateHeatmapSimilarKind,
  WinRateHeatmapSimilarRow,
  WinRateHeatmapStatsFilters,
  WinRateHeatmapStatsPayload,
} from "./types";

interface BloodlineKettoColumn {
  category: WinRateHeatmapBloodlineCategory;
  column: string;
}

export interface CatalogTableSet {
  masterPrimary: "jvd_um" | "nvd_nu" | "nvd_um";
  masterSecondary: "jvd_um" | "nvd_nu" | "nvd_um";
  masterTertiary: "jvd_um" | "nvd_nu" | "nvd_um";
  raceTable: "jvd_ra" | "nvd_ra";
  runnerTable: "jvd_se" | "nvd_se";
  source: CatalogSource;
}

interface FinishCounts {
  places: number;
  shows: number;
  starts: number;
  wins: number;
}

const IDENTIFIER_PATTERN: RegExp = /^[A-Za-z_][A-Za-z0-9_]*$/u;
const DATE_PATTERN: RegExp = /^\d{8}$/u;
const CODE_PATTERN: RegExp = /^\d{2}$/u;
const MIN_STATS_YEARS: number = 1;
const MAX_STATS_YEARS: number = 50;
const UNKNOWN_NAME: string = "不明";

const BLOODLINE_CATEGORIES: ReadonlyArray<WinRateHeatmapBloodlineCategory> = [
  "sire",
  "sireSire",
  "damSire",
  "sireSireSire",
  "sireDamSire",
  "damSireSire",
  "damDamSire",
] satisfies ReadonlyArray<WinRateHeatmapBloodlineCategory>;

const BLOODLINE_KETTO_COLUMNS: ReadonlyArray<BloodlineKettoColumn> = [
  { category: "sire", column: "ketto_joho_01b" },
  { category: "sireSire", column: "ketto_joho_03b" },
  { category: "damSire", column: "ketto_joho_05b" },
  { category: "sireSireSire", column: "ketto_joho_07b" },
  { category: "sireDamSire", column: "ketto_joho_09b" },
  { category: "damSireSire", column: "ketto_joho_11b" },
  { category: "damDamSire", column: "ketto_joho_13b" },
] satisfies ReadonlyArray<BloodlineKettoColumn>;

const TURF_TRACK_CODES: ReadonlyArray<string> = [
  "10",
  "11",
  "12",
  "13",
  "14",
  "15",
  "16",
  "17",
  "18",
  "19",
  "20",
  "21",
  "22",
];
const DIRT_TRACK_CODES: ReadonlyArray<string> = ["23", "24", "25", "26", "29"];
const SAND_TRACK_CODES: ReadonlyArray<string> = ["27", "28"];
const OBSTACLE_TRACK_CODES: ReadonlyArray<string> = [
  "51",
  "52",
  "53",
  "54",
  "55",
  "56",
  "57",
  "58",
  "59",
];
const LEFT_TURN_TRACK_CODES: ReadonlyArray<string> = [
  "11",
  "12",
  "13",
  "14",
  "15",
  "16",
  "23",
  "25",
  "27",
];
const RIGHT_TURN_TRACK_CODES: ReadonlyArray<string> = [
  "17",
  "18",
  "19",
  "20",
  "21",
  "22",
  "24",
  "26",
  "28",
];
const STRAIGHT_TURN_TRACK_CODES: ReadonlyArray<string> = ["10", "29"];

const JRA_TABLES: CatalogTableSet = {
  masterPrimary: "jvd_um",
  masterSecondary: "nvd_um",
  masterTertiary: "nvd_nu",
  raceTable: "jvd_ra",
  runnerTable: "jvd_se",
  source: "jra",
};

const NAR_TABLES: CatalogTableSet = {
  masterPrimary: "nvd_nu",
  masterSecondary: "nvd_um",
  masterTertiary: "jvd_um",
  raceTable: "nvd_ra",
  runnerTable: "nvd_se",
  source: "nar",
};

const requireIdentifier = (value: string, label: string): string => {
  if (!IDENTIFIER_PATTERN.test(value)) {
    throw new Error(`${label} must be an unquoted SQL identifier`);
  }
  return value;
};

const compactUtcDate = (timestamp: number): string =>
  new Date(timestamp).toISOString().slice(0, 10).replaceAll("-", "");

const requireDate = (date: string): string => {
  if (!DATE_PATTERN.test(date)) throw new Error("date must match YYYYMMDD");
  const timestamp = Date.UTC(
    Number(date.slice(0, 4)),
    Number(date.slice(4, 6)) - 1,
    Number(date.slice(6, 8)),
  );
  if (compactUtcDate(timestamp) !== date) throw new Error("date must be a valid calendar date");
  return date;
};

const requireCode = (value: string, label: string): string => {
  if (!CODE_PATTERN.test(value)) throw new Error(`${label} must contain two digits`);
  return value;
};

const requireYears = (years: number): number => {
  if (!Number.isInteger(years) || years < MIN_STATS_YEARS || years > MAX_STATS_YEARS) {
    throw new Error("years must be an integer from 1 to 50");
  }
  return years;
};

export const validateFilters = (
  filters: WinRateHeatmapStatsFilters,
): WinRateHeatmapStatsFilters => {
  requireDate(filters.date);
  requireCode(filters.keibajoCode, "keibajoCode");
  requireCode(filters.raceBango, "raceBango");
  requireYears(filters.years);
  return filters;
};

const namespaceName = (env: R2SqlCatalogConfig): string =>
  requireIdentifier(env.R2_SQL_NAMESPACE, "R2_SQL_NAMESPACE");

export const tableName = (env: R2SqlCatalogConfig, table: string): string =>
  `${namespaceName(env)}.${table}`;

const historyStart = (date: string, years: number): string =>
  `${String(Number(date.slice(0, 4)) - years).padStart(4, "0")}${date.slice(4)}`;

export const currentTables = (source: CatalogSource): CatalogTableSet =>
  source === "jra" ? JRA_TABLES : NAR_TABLES;

export const historyTables = (filters: WinRateHeatmapStatsFilters): CatalogTableSet[] => {
  if (!filters.includeVenue) return [JRA_TABLES, NAR_TABLES];
  if (filters.source === "jra") return [JRA_TABLES];
  return [NAR_TABLES];
};

export const includeOwnerEnabled = (filters: WinRateHeatmapStatsFilters): boolean =>
  filters.includeOwner === true;

export const includeJockeyFrameEnabled = (filters: WinRateHeatmapStatsFilters): boolean =>
  filters.includeJockeyFrame === true;

const sqlStringList = (codes: ReadonlyArray<string>): string =>
  codes.map((code) => `'${code}'`).join(", ");

const paddedTrackCodeSql = (column: string): string =>
  `lpad(btrim(coalesce(${column}, '')), 2, '0')`;

const trackSurfaceSql = (column: string): string => `CASE
    WHEN ${paddedTrackCodeSql(column)} IN (${sqlStringList(TURF_TRACK_CODES)}) THEN '芝'
    WHEN ${paddedTrackCodeSql(column)} IN (${sqlStringList(DIRT_TRACK_CODES)}) THEN 'ダート'
    WHEN ${paddedTrackCodeSql(column)} IN (${sqlStringList(SAND_TRACK_CODES)}) THEN 'サンド'
    WHEN ${paddedTrackCodeSql(column)} IN (${sqlStringList(OBSTACLE_TRACK_CODES)}) THEN '障害'
    ELSE ''
  END`;

const trackTurnSql = (column: string): string => `CASE
    WHEN ${paddedTrackCodeSql(column)} IN (${sqlStringList(LEFT_TURN_TRACK_CODES)}) THEN '左'
    WHEN ${paddedTrackCodeSql(column)} IN (${sqlStringList(RIGHT_TURN_TRACK_CODES)}) THEN '右'
    WHEN ${paddedTrackCodeSql(column)} IN (${sqlStringList(STRAIGHT_TURN_TRACK_CODES)}) THEN '直線'
    ELSE ''
  END`;

const trimmedSql = (expr: string): string =>
  `nullif(btrim(replace(coalesce(${expr}, ''), chr(12288), '')), '')`;

const namedOrUnknownSql = (expr: string): string =>
  `coalesce(${trimmedSql(expr)}, '${UNKNOWN_NAME}')`;

const coalescedKettoSql = (column: string): string =>
  `coalesce(
    ${trimmedSql(`primary_um.${column}`)},
    ${trimmedSql(`secondary_um.${column}`)},
    ${trimmedSql(`tertiary_um.${column}`)},
    '${UNKNOWN_NAME}'
  )`;

export const currentRaceIdentitySql = (filters: WinRateHeatmapStatsFilters): string =>
  `kaisai_nen = '${filters.date.slice(0, 4)}'
    AND kaisai_tsukihi = '${filters.date.slice(4)}'
    AND keibajo_code = '${filters.keibajoCode}'
    AND race_bango = '${filters.raceBango}'`;

const historyDateSql = (filters: WinRateHeatmapStatsFilters, alias: string): string => {
  const start = historyStart(filters.date, filters.years);
  return `concat(${alias}.kaisai_nen, ${alias}.kaisai_tsukihi) < '${filters.date}'
    AND concat(${alias}.kaisai_nen, ${alias}.kaisai_tsukihi) >= '${start}'
    AND ${alias}.kaisai_nen >= '${start.slice(0, 4)}'
    AND ${alias}.kaisai_nen <= '${filters.date.slice(0, 4)}'`;
};

export const similarRaceFilterSql = (filters: WinRateHeatmapStatsFilters): string => {
  const venueSql = filters.includeVenue ? `AND ra.keibajo_code = '${filters.keibajoCode}'` : "";
  const distanceSql = filters.includeDistance
    ? `AND try_cast(nullif(btrim(coalesce(ra.kyori, '')), '') AS INT) = cr.kyori_int
    AND cr.kyori_int IS NOT NULL`
    : "";
  const surfaceSql = filters.includeSurface
    ? `AND (
      ${trackSurfaceSql("cr.track_code")} = ''
      OR (
        ${trackSurfaceSql("ra.track_code")} <> ''
        AND ${trackSurfaceSql("ra.track_code")} = ${trackSurfaceSql("cr.track_code")}
      )
    )`
    : "";
  const turnSql = filters.includeTurn
    ? `AND (
      ${trackTurnSql("cr.track_code")} = ''
      OR (
        ${trackTurnSql("ra.track_code")} <> ''
        AND ${trackTurnSql("ra.track_code")} = ${trackTurnSql("cr.track_code")}
      )
    )`
    : "";
  return `${historyDateSql(filters, "ra")}
    AND ${historyDateSql(filters, "se")}
    ${venueSql}
    ${distanceSql}
    ${surfaceSql}
    ${turnSql}`;
};

export const finishPositionSql = (alias: string): string =>
  `try_cast(nullif(btrim(coalesce(${alias}.kakutei_chakujun, '')), '00') AS INT)`;

const masterJoinsSql = (env: R2SqlCatalogConfig, tables: CatalogTableSet): string =>
  `LEFT JOIN ${tableName(env, tables.masterPrimary)} primary_um
    ON primary_um.ketto_toroku_bango = se.ketto_toroku_bango
  LEFT JOIN ${tableName(env, tables.masterSecondary)} secondary_um
    ON secondary_um.ketto_toroku_bango = se.ketto_toroku_bango
  LEFT JOIN ${tableName(env, tables.masterTertiary)} tertiary_um
    ON tertiary_um.ketto_toroku_bango = se.ketto_toroku_bango`;

export const currentRaceCteSql = (
  env: R2SqlCatalogConfig,
  filters: WinRateHeatmapStatsFilters,
): string =>
  `current_race AS (
  SELECT
    kaisai_nen,
    kaisai_tsukihi,
    keibajo_code,
    race_bango,
    track_code,
    try_cast(nullif(btrim(coalesce(kyori, '')), '') AS INT) AS kyori_int
  FROM ${tableName(env, currentTables(filters.source).raceTable)}
  WHERE ${currentRaceIdentitySql(filters)}
)`;

const currentBloodlineSelectSql = (): string =>
  BLOODLINE_KETTO_COLUMNS.map(
    (entry) => `${coalescedKettoSql(entry.column)} AS ${entry.category}`,
  ).join(",\n    ");

const currentBloodlineUnpivotSql = (): string =>
  BLOODLINE_KETTO_COLUMNS.map(
    (entry) =>
      `SELECT umaban, '${entry.category}' AS category, ${entry.category} AS name
    FROM current_entries
    WHERE ${entry.category} <> '${UNKNOWN_NAME}'`,
  ).join("\n  UNION ALL\n  ");

const bloodlineMatchSql = (): string =>
  BLOODLINE_KETTO_COLUMNS.map(
    (entry) =>
      `(tn.category = '${entry.category}' AND ${coalescedKettoSql(entry.column)} = tn.name)`,
  ).join("\n    OR ");

const bloodlineHistoryArmSql = (
  env: R2SqlCatalogConfig,
  filters: WinRateHeatmapStatsFilters,
  tables: CatalogTableSet,
): string => `SELECT
    tn.category,
    tn.name,
    ${finishPositionSql("se")} AS finish_position
  FROM ${tableName(env, tables.runnerTable)} se
  INNER JOIN ${tableName(env, tables.raceTable)} ra
    ON ra.kaisai_nen = se.kaisai_nen
    AND ra.kaisai_tsukihi = se.kaisai_tsukihi
    AND ra.keibajo_code = se.keibajo_code
    AND ra.race_bango = se.race_bango
  CROSS JOIN current_race cr
  ${masterJoinsSql(env, tables)}
  INNER JOIN target_names tn
    ON ${bloodlineMatchSql()}
  WHERE ${similarRaceFilterSql(filters)}
    AND ${finishPositionSql("se")} IS NOT NULL
    AND ${finishPositionSql("se")} > 0
    AND ${trimmedSql("se.ketto_toroku_bango")} IS NOT NULL
    AND NOT EXISTS (
      SELECT 1
      FROM current_entries ce
      WHERE ce.ketto_toroku_bango = se.ketto_toroku_bango
    )`;

const similarHistoryArmSql = (
  env: R2SqlCatalogConfig,
  filters: WinRateHeatmapStatsFilters,
  tables: CatalogTableSet,
): string => {
  const ownerMatchSql =
    includeOwnerEnabled(filters) === true
      ? ` OR (
      tn.kind = 'owner'
      AND ${namedOrUnknownSql("se.banushimei")} = tn.name
    )`
      : "";
  const jockeyFrameMatchSql =
    includeJockeyFrameEnabled(filters) === true
      ? ` OR (
      tn.kind = 'jockeyFrame'
      AND ${namedOrUnknownSql("se.kishumei_ryakusho")} = tn.name
      AND ${paddedTrackCodeSql("se.wakuban")} = tn.frame
    )`
      : "";
  const frameSelectSql =
    includeJockeyFrameEnabled(filters) === true ? `,\n    coalesce(tn.frame, '') AS frame` : "";
  return `SELECT
    tn.kind,
    tn.name${frameSelectSql},
    ${finishPositionSql("se")} AS finish_position
  FROM ${tableName(env, tables.runnerTable)} se
  INNER JOIN ${tableName(env, tables.raceTable)} ra
    ON ra.kaisai_nen = se.kaisai_nen
    AND ra.kaisai_tsukihi = se.kaisai_tsukihi
    AND ra.keibajo_code = se.keibajo_code
    AND ra.race_bango = se.race_bango
  CROSS JOIN current_race cr
  INNER JOIN target_people tn
    ON (
      tn.kind = 'jockey'
      AND ${namedOrUnknownSql("se.kishumei_ryakusho")} = tn.name
    ) OR (
      tn.kind = 'trainer'
      AND ${namedOrUnknownSql("se.chokyoshimei_ryakusho")} = tn.name
    )${ownerMatchSql}${jockeyFrameMatchSql}
  WHERE ${similarRaceFilterSql(filters)}
    AND ${finishPositionSql("se")} IS NOT NULL
    AND ${finishPositionSql("se")} > 0`;
};

const aggregateSelectSql = (groupColumns: string): string =>
  `SELECT
    ${groupColumns},
    count(*) AS starts,
    sum(CASE WHEN finish_position = 1 THEN 1 ELSE 0 END) AS wins,
    sum(CASE WHEN finish_position IN (1, 2) THEN 1 ELSE 0 END) AS places,
    sum(CASE WHEN finish_position IN (1, 2, 3) THEN 1 ELSE 0 END) AS shows
  FROM matched_history
  GROUP BY ${groupColumns}`;

export const unionHistorySql = (arms: ReadonlyArray<string>): string =>
  arms.join("\n  UNION ALL\n  ");

export const buildWinRateHeatmapBloodlineQuery = (
  env: R2SqlCatalogConfig,
  filters: WinRateHeatmapStatsFilters,
): string => {
  const checked = validateFilters(filters);
  const current = currentTables(checked.source);
  return `
WITH ${currentRaceCteSql(env, checked)},
current_entries AS (
  SELECT
    try_cast(nullif(btrim(coalesce(se.umaban, '')), '') AS INT) AS umaban,
    se.ketto_toroku_bango,
    ${currentBloodlineSelectSql()}
  FROM ${tableName(env, current.runnerTable)} se
  ${masterJoinsSql(env, current)}
  WHERE ${currentRaceIdentitySql(checked)}
    AND try_cast(nullif(btrim(coalesce(se.umaban, '')), '') AS INT) IS NOT NULL
    AND ${trimmedSql("se.ketto_toroku_bango")} IS NOT NULL
),
current_bloodlines AS (
  ${currentBloodlineUnpivotSql()}
),
target_names AS (
  SELECT DISTINCT category, name
  FROM current_bloodlines
),
matched_history AS (
  ${unionHistorySql(
    historyTables(checked).map((tables) => bloodlineHistoryArmSql(env, checked, tables)),
  )}
),
stats AS (
  ${aggregateSelectSql("category, name")}
)
SELECT
  cb.category,
  cb.umaban,
  cb.name,
  coalesce(stats.starts, 0) AS starts,
  coalesce(stats.wins, 0) AS wins,
  coalesce(stats.places, 0) AS places,
  coalesce(stats.shows, 0) AS shows
FROM current_bloodlines cb
LEFT JOIN stats
  ON stats.category = cb.category
  AND stats.name = cb.name
ORDER BY cb.umaban, cb.category`;
};

export const buildWinRateHeatmapSimilarQuery = (
  env: R2SqlCatalogConfig,
  filters: WinRateHeatmapStatsFilters,
): string => {
  const checked = validateFilters(filters);
  const current = currentTables(checked.source);
  const withFrame = includeJockeyFrameEnabled(checked) === true;
  const ownerSelectSql =
    includeOwnerEnabled(checked) === true
      ? `,\n    ${namedOrUnknownSql("se.banushimei")} AS owner`
      : "";
  const wakubanSelectSql = withFrame ? `,\n    ${paddedTrackCodeSql("se.wakuban")} AS wakuban` : "";
  const peopleFrameSql = withFrame ? ", '' AS frame" : "";
  const ownerPeopleSql =
    includeOwnerEnabled(checked) === true
      ? `
  UNION ALL
  SELECT umaban, 'owner' AS kind, owner AS name${peopleFrameSql}
  FROM current_entries`
      : "";
  const jockeyFramePeopleSql = withFrame
    ? `
  UNION ALL
  SELECT umaban, 'jockeyFrame' AS kind, jockey AS name, wakuban AS frame
  FROM current_entries
  WHERE wakuban <> '' AND wakuban <> '00'`
    : "";
  const targetPeopleSql = withFrame
    ? `SELECT DISTINCT kind, name, frame
  FROM current_people`
    : `SELECT DISTINCT kind, name
  FROM current_people`;
  const statsGroupSql = withFrame ? "kind, name, frame" : "kind, name";
  const statsJoinSql = withFrame
    ? `ON stats.kind = cp.kind
  AND stats.name = cp.name
  AND coalesce(stats.frame, '') = coalesce(cp.frame, '')`
    : `ON stats.kind = cp.kind
  AND stats.name = cp.name`;
  return `
WITH ${currentRaceCteSql(env, checked)},
current_entries AS (
  SELECT
    try_cast(nullif(btrim(coalesce(se.umaban, '')), '') AS INT) AS umaban,
    ${namedOrUnknownSql("se.kishumei_ryakusho")} AS jockey,
    ${namedOrUnknownSql("se.chokyoshimei_ryakusho")} AS trainer${ownerSelectSql}${wakubanSelectSql}
  FROM ${tableName(env, current.runnerTable)} se
  WHERE ${currentRaceIdentitySql(checked)}
    AND try_cast(nullif(btrim(coalesce(se.umaban, '')), '') AS INT) IS NOT NULL
),
current_people AS (
  SELECT umaban, 'jockey' AS kind, jockey AS name${peopleFrameSql}
  FROM current_entries
  UNION ALL
  SELECT umaban, 'trainer' AS kind, trainer AS name${peopleFrameSql}
  FROM current_entries${ownerPeopleSql}${jockeyFramePeopleSql}
),
target_people AS (
  ${targetPeopleSql}
),
matched_history AS (
  ${unionHistorySql(
    historyTables(checked).map((tables) => similarHistoryArmSql(env, checked, tables)),
  )}
),
stats AS (
  ${aggregateSelectSql(statsGroupSql)}
)
SELECT
  cp.kind,
  cp.umaban,
  cp.name,
  coalesce(stats.starts, 0) AS starts,
  coalesce(stats.wins, 0) AS wins,
  coalesce(stats.places, 0) AS places,
  coalesce(stats.shows, 0) AS shows
FROM current_people cp
LEFT JOIN stats
  ${statsJoinSql}
ORDER BY cp.umaban, cp.kind`;
};

const isBloodlineCategory = (value: string): value is WinRateHeatmapBloodlineCategory =>
  BLOODLINE_CATEGORIES.some((category) => category === value);

const isSimilarKind = (value: string): value is WinRateHeatmapSimilarKind =>
  value === "jockey" || value === "jockeyFrame" || value === "owner" || value === "trainer";

const requiredString = (value: unknown, field: string): string => {
  if (typeof value === "string" && value.length > 0) return value;
  if (typeof value === "number" || typeof value === "bigint" || typeof value === "boolean") {
    return String(value);
  }
  throw new Error(`R2 SQL row is missing ${field}`);
};

const requiredNumber = (value: unknown, field: string): number => {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "bigint") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  throw new Error(`R2 SQL row is missing ${field}`);
};

const requiredInteger = (value: unknown, field: string): number => {
  const parsed = requiredNumber(value, field);
  if (!Number.isInteger(parsed)) throw new Error(`R2 SQL row is missing ${field}`);
  return parsed;
};

const finishCounts = (row: Record<string, unknown>): FinishCounts => ({
  places: requiredInteger(row.places, "places"),
  shows: requiredInteger(row.shows, "shows"),
  starts: requiredInteger(row.starts, "starts"),
  wins: requiredInteger(row.wins, "wins"),
});

export const normaliseBloodlineRow = (raw: Record<string, unknown>): WinRateHeatmapBloodlineRow => {
  const category = requiredString(raw.category, "category");
  if (!isBloodlineCategory(category)) {
    throw new Error(`R2 SQL row has invalid category: ${category}`);
  }
  const counts = finishCounts(raw);
  return {
    category,
    details: [],
    name: requiredString(raw.name, "name"),
    places: counts.places,
    shows: counts.shows,
    starts: counts.starts,
    umaban: requiredInteger(raw.umaban, "umaban"),
    wins: counts.wins,
  };
};

export const normaliseSimilarRow = (raw: Record<string, unknown>): WinRateHeatmapSimilarRow => {
  const kind = requiredString(raw.kind, "kind");
  if (!isSimilarKind(kind)) {
    throw new Error(`R2 SQL row has invalid kind: ${kind}`);
  }
  const counts = finishCounts(raw);
  return {
    details: [],
    kind,
    name: requiredString(raw.name, "name"),
    places: counts.places,
    shows: counts.shows,
    starts: counts.starts,
    umaban: requiredInteger(raw.umaban, "umaban"),
    wins: counts.wins,
  };
};

export const normaliseWinRateHeatmapStatsPayload = (input: {
  bloodlineRows: ReadonlyArray<Record<string, unknown>>;
  similarRows: ReadonlyArray<Record<string, unknown>>;
}): WinRateHeatmapStatsPayload => ({
  bloodlineRows: input.bloodlineRows.map(normaliseBloodlineRow),
  similarRows: input.similarRows.map(normaliseSimilarRow),
});
