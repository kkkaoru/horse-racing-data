// Run with bun (bunx vitest).

import type {
  ConditionFinishPositionStatsRow,
  ConditionFrameStatsRow,
  ConditionHistoryStatsPayload,
  ConditionRaceTimeStats,
  ConditionWeightClassStatsRow,
  R2SqlCatalogConfig,
  WinRateHeatmapStatsFilters,
} from "./types";
import {
  currentRaceCteSql,
  currentRaceIdentitySql,
  currentTables,
  finishPositionSql,
  historyTables,
  similarRaceFilterSql,
  tableName,
  unionHistorySql,
  validateFilters,
  type CatalogTableSet,
} from "./win-rate-heatmap-stats";

type WeightClassKind = "body" | "carried";

const RATE_SCALE: number = 10;
const SCORE_SCALE: number = 100;
const BAN_EI_KEIBAJO_CODES: ReadonlyArray<string> = ["81", "82", "83", "84"];
const EMPTY_DETAILS: [] = [];
const EMPTY_CORRELATION_ROWS: [] = [];
const EMPTY_TARGET_RACES: [] = [];

const BODY_WEIGHT_CLASS_SQL: string = `CASE
    WHEN body_weight < 400 THEN 'le399'
    WHEN body_weight < 420 THEN '400-419'
    WHEN body_weight < 440 THEN '420-439'
    WHEN body_weight < 460 THEN '440-459'
    WHEN body_weight < 480 THEN '460-479'
    WHEN body_weight < 500 THEN '480-499'
    WHEN body_weight < 520 THEN '500-519'
    WHEN body_weight < 540 THEN '520-539'
    ELSE 'ge540'
  END`;

const CARRIED_WEIGHT_CLASS_SQL: string = `CASE
    WHEN carried_weight <= 49 THEN 'le49'
    WHEN carried_weight <= 51 THEN '49.5-51'
    WHEN carried_weight <= 53 THEN '51.5-53'
    WHEN carried_weight <= 55 THEN '53.5-55'
    WHEN carried_weight <= 57 THEN '55.5-57'
    ELSE '57.5-59'
  END`;

export const isBanEiKeibajo = (keibajoCode: string): boolean =>
  BAN_EI_KEIBAJO_CODES.some((code) => code === keibajoCode);

const roundTo = (value: number, scale: number): number => Math.round(value * scale) / scale;

const rateFromCounts = (count: number, starts: number): number =>
  starts === 0 ? 0 : roundTo((count * 100) / starts, RATE_SCALE);

const optionalNumber = (value: unknown): number | null => {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "bigint") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

const requiredInteger = (value: unknown, field: string): number => {
  const parsed = optionalNumber(value);
  if (parsed === null || !Number.isInteger(parsed)) {
    throw new Error(`R2 SQL row is missing ${field}`);
  }
  return parsed;
};

const requiredString = (value: unknown, field: string): string => {
  if (typeof value === "string" && value.length > 0) return value;
  if (typeof value === "number" || typeof value === "bigint" || typeof value === "boolean") {
    return String(value);
  }
  throw new Error(`R2 SQL row is missing ${field}`);
};

const roundedOrNull = (value: unknown, scale: number): number | null => {
  const parsed = optionalNumber(value);
  return parsed === null ? null : roundTo(parsed, scale);
};

const integerSelect = (expr: string, empty: string): string =>
  `try_cast(nullif(btrim(coalesce(${expr}, '')), '${empty}') AS INT)`;

const doubleSelect = (expr: string): string =>
  `try_cast(nullif(btrim(coalesce(${expr}, '')), '') AS DOUBLE)`;

const matchedHistoryArmSql = (input: {
  env: R2SqlCatalogConfig;
  extraJoin: string;
  extraWhere: string;
  filters: WinRateHeatmapStatsFilters;
  selectList: string;
  tables: CatalogTableSet;
}): string => `SELECT
    ${input.selectList}
  FROM ${tableName(input.env, input.tables.runnerTable)} se
  INNER JOIN ${tableName(input.env, input.tables.raceTable)} ra
    ON ra.kaisai_nen = se.kaisai_nen
    AND ra.kaisai_tsukihi = se.kaisai_tsukihi
    AND ra.keibajo_code = se.keibajo_code
    AND ra.race_bango = se.race_bango
  CROSS JOIN current_race cr
  ${input.extraJoin}
  WHERE ${similarRaceFilterSql(input.filters)}
    AND ${finishPositionSql("se")} IS NOT NULL
    AND ${finishPositionSql("se")} > 0
    ${input.extraWhere}`;

const classCountSelectSql = (groupColumn: string): string => `SELECT
    ${groupColumn} AS class_key,
    count(*) AS starts,
    sum(CASE WHEN finish_position = 1 THEN 1 ELSE 0 END) AS win_count,
    sum(CASE WHEN finish_position <= 2 THEN 1 ELSE 0 END) AS quinella_count,
    sum(CASE WHEN finish_position <= 3 THEN 1 ELSE 0 END) AS show_count
  FROM classed
  GROUP BY ${groupColumn}
  ORDER BY ${groupColumn}`;

export const buildConditionFrameStatsQuery = (
  env: R2SqlCatalogConfig,
  filters: WinRateHeatmapStatsFilters,
): string => {
  const checked = validateFilters(filters);
  const current = currentTables(checked.source);
  const selectList = `${integerSelect("se.wakuban", "00")} AS frame_number,
    ${integerSelect("ra.shusso_tosu", "")} AS runner_count,
    ${finishPositionSql("se")} AS finish_position,
    ${integerSelect("se.tansho_ninkijun", "00")} AS popularity`;
  return `
WITH ${currentRaceCteSql(env, checked)},
current_frames AS (
  SELECT DISTINCT ${integerSelect("se.wakuban", "00")} AS frame_number
  FROM ${tableName(env, current.runnerTable)} se
  WHERE ${currentRaceIdentitySql(checked)}
    AND ${integerSelect("se.wakuban", "00")} IS NOT NULL
    AND ${integerSelect("se.wakuban", "00")} > 0
),
matched_history AS (
  ${unionHistorySql(
    historyTables(checked).map((tables) =>
      matchedHistoryArmSql({
        env,
        extraJoin:
          "INNER JOIN current_frames cf ON cf.frame_number = try_cast(nullif(btrim(coalesce(se.wakuban, '')), '00') AS INT)",
        extraWhere: "",
        filters: checked,
        selectList,
        tables,
      }),
    ),
  )}
)
SELECT
  frame_number,
  max(runner_count) AS runner_count,
  count(*) AS count,
  sum(CASE WHEN finish_position = 1 THEN 1 ELSE 0 END) AS win_count,
  sum(CASE WHEN finish_position <= 2 THEN 1 ELSE 0 END) AS quinella_count,
  sum(CASE WHEN finish_position <= 3 THEN 1 ELSE 0 END) AS show_count,
  avg(finish_position) AS average_finish,
  approx_percentile_cont(finish_position, 0.5) AS median_finish,
  avg(CASE WHEN popularity > 0 THEN popularity END) AS average_popularity,
  approx_percentile_cont(CASE WHEN popularity > 0 THEN popularity END, 0.5) AS median_popularity
FROM matched_history
WHERE frame_number IS NOT NULL
GROUP BY frame_number
ORDER BY frame_number`;
};

export const buildConditionWeightClassStatsQuery = (input: {
  env: R2SqlCatalogConfig;
  filters: WinRateHeatmapStatsFilters;
  kind: WeightClassKind;
}): string => {
  const checked = validateFilters(input.filters);
  const isBody = input.kind === "body";
  const weightExpr = isBody
    ? `${integerSelect("se.bataiju", "000")} AS body_weight`
    : `${doubleSelect("se.futan_juryo")} / 10.0 AS carried_weight`;
  const classExpr = isBody ? BODY_WEIGHT_CLASS_SQL : CARRIED_WEIGHT_CLASS_SQL;
  const weightFilter = isBody
    ? "AND try_cast(nullif(btrim(coalesce(se.bataiju, '')), '000') AS INT) > 0"
    : "AND try_cast(nullif(btrim(coalesce(se.futan_juryo, '')), '') AS DOUBLE) / 10.0 > 0";
  return `
WITH ${currentRaceCteSql(input.env, checked)},
matched_history AS (
  ${unionHistorySql(
    historyTables(checked).map((tables) =>
      matchedHistoryArmSql({
        env: input.env,
        extraJoin: "",
        extraWhere: `AND se.keibajo_code NOT IN ('81', '82', '83', '84')
    ${weightFilter}`,
        filters: checked,
        selectList: `${finishPositionSql("se")} AS finish_position,
    ${weightExpr}`,
        tables,
      }),
    ),
  )}
),
classed AS (
  SELECT
    ${classExpr} AS class_key,
    finish_position
  FROM matched_history
)
${classCountSelectSql("class_key")}`;
};

export const buildConditionFinishPositionStatsQuery = (
  env: R2SqlCatalogConfig,
  filters: WinRateHeatmapStatsFilters,
): string => {
  const checked = validateFilters(filters);
  return `
WITH ${currentRaceCteSql(env, checked)},
matched_history AS (
  ${unionHistorySql(
    historyTables(checked).map((tables) =>
      matchedHistoryArmSql({
        env,
        extraJoin: "",
        extraWhere: `AND ${finishPositionSql("se")} IN (1, 2, 3, 4, 5)`,
        filters: checked,
        selectList: `${finishPositionSql("se")} AS finish_position,
    ${integerSelect("se.tansho_ninkijun", "00")} AS popularity,
    ${doubleSelect("se.tansho_odds")} / 10.0 AS odds`,
        tables,
      }),
    ),
  )}
)
SELECT
  finish_position,
  count(*) AS count,
  avg(CASE WHEN popularity > 0 THEN popularity END) AS average_popularity,
  approx_percentile_cont(CASE WHEN popularity > 0 THEN popularity END, 0.5) AS median_popularity,
  avg(CASE WHEN odds > 0 THEN odds END) AS average_odds,
  approx_percentile_cont(CASE WHEN odds > 0 THEN odds END, 0.5) AS median_odds
FROM matched_history
GROUP BY finish_position
ORDER BY finish_position`;
};

export const buildConditionRaceTimeStatsQuery = (
  env: R2SqlCatalogConfig,
  filters: WinRateHeatmapStatsFilters,
): string => {
  const checked = validateFilters(filters);
  return `
WITH ${currentRaceCteSql(env, checked)},
matched_history AS (
  ${unionHistorySql(
    historyTables(checked).map((tables) =>
      matchedHistoryArmSql({
        env,
        extraJoin: "",
        extraWhere: `AND ${finishPositionSql("se")} = 1
    AND ${doubleSelect("se.soha_time")} IS NOT NULL
    AND ${doubleSelect("se.soha_time")} > 0`,
        filters: checked,
        selectList: `${doubleSelect("se.soha_time")} AS race_time,
    ${doubleSelect("se.kohan_3f")} AS kohan_3f`,
        tables,
      }),
    ),
  )}
)
SELECT
  count(*) AS race_count,
  min(race_time) AS fastest_race_time,
  min(kohan_3f) AS fastest_kohan_3f,
  avg(race_time) AS average_race_time,
  avg(kohan_3f) AS average_kohan_3f,
  approx_percentile_cont(race_time, 0.5) AS median_race_time,
  approx_percentile_cont(kohan_3f, 0.5) AS median_kohan_3f
FROM matched_history`;
};

const rawFrameScore = (averageFinish: number | null, medianFinish: number | null): number => {
  const averagePart = averageFinish !== null && averageFinish !== 0 ? 1 / averageFinish : 0;
  const medianPart = medianFinish !== null && medianFinish !== 0 ? 1 / medianFinish : 0;
  return averagePart + medianPart;
};

const normalizedFrameScore = (raw: number, minScore: number, maxScore: number): number => {
  if (maxScore > minScore) return (raw - minScore) / (maxScore - minScore);
  return raw > 0 ? 1 : 0;
};

const scoredFrameRows = (
  rows: ReadonlyArray<Omit<ConditionFrameStatsRow, "score">>,
): ConditionFrameStatsRow[] => {
  const rawScores = rows.map((row) => rawFrameScore(row.averageFinish, row.medianFinish));
  const minScore = rawScores.reduce(
    (current, score) => (score < current ? score : current),
    Number.POSITIVE_INFINITY,
  );
  const maxScore = rawScores.reduce(
    (current, score) => (score > current ? score : current),
    Number.NEGATIVE_INFINITY,
  );
  return rows.map((row) => {
    const raw = rawFrameScore(row.averageFinish, row.medianFinish);
    return {
      ...row,
      score: roundTo(normalizedFrameScore(raw, minScore, maxScore), SCORE_SCALE),
    };
  });
};

export const normaliseWeightClassRow = (
  raw: Record<string, unknown>,
): ConditionWeightClassStatsRow => {
  const starts = requiredInteger(raw.starts, "starts");
  const winCount = requiredInteger(raw.win_count, "win_count");
  const quinellaCount = requiredInteger(raw.quinella_count, "quinella_count");
  const showCount = requiredInteger(raw.show_count, "show_count");
  return {
    key: requiredString(raw.class_key, "class_key"),
    quinellaCount,
    quinellaRate: rateFromCounts(quinellaCount, starts),
    showCount,
    showRate: rateFromCounts(showCount, starts),
    starts,
    winCount,
    winRate: rateFromCounts(winCount, starts),
  };
};

export const normaliseFinishPositionRow = (
  raw: Record<string, unknown>,
): ConditionFinishPositionStatsRow => ({
  averageOdds: roundedOrNull(raw.average_odds, RATE_SCALE),
  averagePopularity: roundedOrNull(raw.average_popularity, RATE_SCALE),
  count: requiredInteger(raw.count, "count"),
  details: EMPTY_DETAILS,
  finishPosition: requiredInteger(raw.finish_position, "finish_position"),
  medianOdds: roundedOrNull(raw.median_odds, RATE_SCALE),
  medianPopularity: roundedOrNull(raw.median_popularity, RATE_SCALE),
});

const normaliseFrameRow = (raw: Record<string, unknown>): Omit<ConditionFrameStatsRow, "score"> => {
  const count = requiredInteger(raw.count, "count");
  const winCount = requiredInteger(raw.win_count, "win_count");
  const quinellaCount = requiredInteger(raw.quinella_count, "quinella_count");
  const showCount = requiredInteger(raw.show_count, "show_count");
  return {
    averageFinish: roundedOrNull(raw.average_finish, RATE_SCALE),
    averagePopularity: roundedOrNull(raw.average_popularity, RATE_SCALE),
    count,
    details: EMPTY_DETAILS,
    frameNumber: requiredString(raw.frame_number, "frame_number"),
    medianFinish: roundedOrNull(raw.median_finish, RATE_SCALE),
    medianPopularity: roundedOrNull(raw.median_popularity, RATE_SCALE),
    quinellaCount,
    quinellaRate: rateFromCounts(quinellaCount, count),
    runnerCount: optionalNumber(raw.runner_count),
    showCount,
    showRate: rateFromCounts(showCount, count),
    winCount,
    winRate: rateFromCounts(winCount, count),
  };
};

export const emptyRaceTimeStats = (): ConditionRaceTimeStats => ({
  averageKohan3f: null,
  averageRaceTime: null,
  correlationRows: EMPTY_CORRELATION_ROWS,
  fastestDetail: null,
  fastestKohan3f: null,
  fastestRaceTime: null,
  medianKohan3f: null,
  medianRaceTime: null,
  raceCount: 0,
  targetRaces: EMPTY_TARGET_RACES,
});

export const normaliseRaceTimeStats = (
  raw: Record<string, unknown> | undefined,
): ConditionRaceTimeStats => {
  if (raw === undefined) return emptyRaceTimeStats();
  return {
    averageKohan3f: roundedOrNull(raw.average_kohan_3f, RATE_SCALE),
    averageRaceTime: roundedOrNull(raw.average_race_time, RATE_SCALE),
    correlationRows: EMPTY_CORRELATION_ROWS,
    fastestDetail: null,
    fastestKohan3f: roundedOrNull(raw.fastest_kohan_3f, RATE_SCALE),
    fastestRaceTime: roundedOrNull(raw.fastest_race_time, RATE_SCALE),
    medianKohan3f: roundedOrNull(raw.median_kohan_3f, RATE_SCALE),
    medianRaceTime: roundedOrNull(raw.median_race_time, RATE_SCALE),
    raceCount: requiredInteger(raw.race_count, "race_count"),
    targetRaces: EMPTY_TARGET_RACES,
  };
};

export const normaliseConditionHistoryStatsPayload = (input: {
  carriedRows: ReadonlyArray<Record<string, unknown>>;
  finishRows: ReadonlyArray<Record<string, unknown>>;
  frameRows: ReadonlyArray<Record<string, unknown>>;
  raceTimeRows: ReadonlyArray<Record<string, unknown>>;
  weightRows: ReadonlyArray<Record<string, unknown>>;
}): ConditionHistoryStatsPayload => ({
  carriedWeightClassStats: input.carriedRows.map(normaliseWeightClassRow),
  finishPositionStats: input.finishRows.map(normaliseFinishPositionRow),
  frameStats: scoredFrameRows(input.frameRows.map(normaliseFrameRow)),
  raceTimeStats: normaliseRaceTimeStats(input.raceTimeRows[0]),
  weightClassStats: input.weightRows.map(normaliseWeightClassRow),
});
