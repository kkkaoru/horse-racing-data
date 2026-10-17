// Run with bun (bunx vitest).

import type {
  ConditionCorrelationRow,
  ConditionFinishPositionDetail,
  ConditionFinishPositionStatsRow,
  ConditionFrameStatsRow,
  ConditionHistoryStatsPayload,
  ConditionRaceTimeStats,
  ConditionTargetRace,
  ConditionWeightClassStatsRow,
  R2SqlCatalogConfig,
  WinRateHeatmapStatsFilters,
} from "./types";
import { isCurrentArc } from "./arc-cohort";
import { encodedRaceTimeTenthsSql } from "./race-time";
import { hexTripletSql } from "./time-score-sql";
import {
  overseasGateJoinSql,
  usesOverseasGate,
  currentRaceCteSql,
  currentRaceIdentitySql,
  currentTables,
  finishPositionSql,
  rateFinishPositionSql,
  rateStartPredicateSql,
  historyKyoriIntSql,
  historyTables,
  isBanEiKeibajo,
  cellRouterDistanceBandFilterSql,
  exactHistoryKyoriFilterSql,
  similarRaceFilterSql,
  tableName,
  unionHistorySql,
  validateFilters,
  type CatalogTableSet,
} from "./win-rate-heatmap-stats";

export { isBanEiKeibajo };

type WeightClassKind = "body" | "carried" | "change";

const RATE_SCALE: number = 10;
const SCORE_SCALE: number = 100;
const EMPTY_DETAILS: [] = [];
const EMPTY_CORRELATION_ROWS: [] = [];
const TARGET_RACE_LIMIT: number = 500;
const FINISH_DETAIL_LIMIT: number = 100;

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

const BAN_EI_BODY_WEIGHT_CLASS_SQL: string = `CASE
    WHEN body_weight < 900 THEN 'le899'
    WHEN body_weight < 950 THEN '900-949'
    WHEN body_weight < 1000 THEN '950-999'
    WHEN body_weight < 1050 THEN '1000-1049'
    WHEN body_weight < 1100 THEN '1050-1099'
    WHEN body_weight < 1150 THEN '1100-1149'
    WHEN body_weight < 1200 THEN '1150-1199'
    ELSE 'ge1200'
  END`;

const BAN_EI_HEX_BODY_WEIGHT_SQL: string = hexTripletSql("upper(btrim(coalesce(se.bataiju, '')))");

// Body weight change bands, lower bound inclusive: +20 and over, +10 to
// under +20, ... -2 to under +2, ... -20 to under -10, under -20.
const WEIGHT_CHANGE_CLASS_SQL: string = `CASE
    WHEN weight_change >= 20 THEN 'plus20'
    WHEN weight_change >= 10 THEN 'plus10'
    WHEN weight_change >= 5 THEN 'plus5'
    WHEN weight_change >= 2 THEN 'plus2'
    WHEN weight_change >= -2 THEN 'even'
    WHEN weight_change >= -5 THEN 'minus2'
    WHEN weight_change >= -10 THEN 'minus5'
    WHEN weight_change >= -20 THEN 'minus10'
    ELSE 'minus20'
  END`;

const CARRIED_WEIGHT_CLASS_SQL: string = `CASE
    WHEN carried_weight <= 49 THEN 'le49'
    WHEN carried_weight <= 51 THEN '49.5-51'
    WHEN carried_weight <= 53 THEN '51.5-53'
    WHEN carried_weight <= 55 THEN '53.5-55'
    WHEN carried_weight <= 57 THEN '55.5-57'
    ELSE '57.5-59'
  END`;

const BAN_EI_VENUE_CONDITION: string = "se.keibajo_code IN ('81', '82', '83', '84')";
const NON_BAN_EI_VENUE_CONDITION: string = "se.keibajo_code NOT IN ('81', '82', '83', '84')";
const BAN_EI_VENUE_ONLY_SQL: string = `AND ${BAN_EI_VENUE_CONDITION}`;
const NON_BAN_EI_VENUE_SQL: string = `AND ${NON_BAN_EI_VENUE_CONDITION}`;
const CORRELATION_ODDS_SCALE: number = 10;
const TRIMMED_BODY_WEIGHT_SQL: string = "upper(btrim(coalesce(se.bataiju, '')))";
const DECIMAL_BODY_WEIGHT_SQL: string =
  "try_cast(nullif(btrim(coalesce(se.bataiju, '')), '000') AS INT)";
const CARRIED_WEIGHT_SQL: string =
  "try_cast(nullif(btrim(coalesce(se.futan_juryo, '')), '') AS DOUBLE) / 10.0";
const TRIMMED_WEIGHT_CHANGE_SQL: string = "upper(btrim(coalesce(se.zogen_sa, '')))";
// Ban-ei records the change in hex ('00A' = 10kg); JRA and NAR use decimal.
const BAN_EI_HEX_WEIGHT_CHANGE_SQL: string = hexTripletSql(TRIMMED_WEIGHT_CHANGE_SQL);
const DECIMAL_WEIGHT_CHANGE_SQL: string = `try_cast(${TRIMMED_WEIGHT_CHANGE_SQL} AS INT)`;

interface WeightClassColumn {
  classColumn: string;
  classExpr: string;
  kind: WeightClassKind;
  valueColumn: string;
  valueExpr: string;
}

interface WeightClassCounts {
  quinellaCount: number;
  showCount: number;
  starts: number;
  winCount: number;
}

export interface ConditionWeightClassRowGroups {
  carriedRows: Record<string, unknown>[];
  weightChangeRows: Record<string, unknown>[];
  weightRows: Record<string, unknown>[];
}

const validValueSql = (condition: string, value: string): string =>
  `CASE WHEN ${condition} THEN ${value} ELSE NULL END`;

const signedWeightChangeSql = (magnitude: string): string =>
  `CASE WHEN btrim(coalesce(se.zogen_fugo, '')) = '-' THEN -(${magnitude}) ELSE (${magnitude}) END`;

const BODY_WEIGHT_COLUMN: WeightClassColumn = {
  classColumn: "body_class",
  classExpr: BODY_WEIGHT_CLASS_SQL,
  kind: "body",
  valueColumn: "body_weight",
  valueExpr: validValueSql(`${DECIMAL_BODY_WEIGHT_SQL} > 0`, DECIMAL_BODY_WEIGHT_SQL),
};

const BAN_EI_BODY_WEIGHT_COLUMN: WeightClassColumn = {
  classColumn: "body_class",
  classExpr: BAN_EI_BODY_WEIGHT_CLASS_SQL,
  kind: "body",
  valueColumn: "body_weight",
  valueExpr: validValueSql(
    `${TRIMMED_BODY_WEIGHT_SQL} NOT IN ('', '000', 'FFF') AND (${BAN_EI_HEX_BODY_WEIGHT_SQL}) > 0`,
    `(${BAN_EI_HEX_BODY_WEIGHT_SQL})`,
  ),
};

const CARRIED_WEIGHT_COLUMN: WeightClassColumn = {
  classColumn: "carried_class",
  classExpr: CARRIED_WEIGHT_CLASS_SQL,
  kind: "carried",
  valueColumn: "carried_weight",
  valueExpr: validValueSql(`${CARRIED_WEIGHT_SQL} > 0`, CARRIED_WEIGHT_SQL),
};

// Blank is a debut or missing weigh-in; '999' (decimal) and 'FFF' (hex) mark
// an unmeasured change.
const WEIGHT_CHANGE_COLUMN: WeightClassColumn = {
  classColumn: "change_class",
  classExpr: WEIGHT_CHANGE_CLASS_SQL,
  kind: "change",
  valueColumn: "weight_change",
  valueExpr: validValueSql(
    `${TRIMMED_WEIGHT_CHANGE_SQL} NOT IN ('', '999') AND (${DECIMAL_WEIGHT_CHANGE_SQL}) IS NOT NULL`,
    signedWeightChangeSql(DECIMAL_WEIGHT_CHANGE_SQL),
  ),
};

const BAN_EI_WEIGHT_CHANGE_COLUMN: WeightClassColumn = {
  classColumn: "change_class",
  classExpr: WEIGHT_CHANGE_CLASS_SQL,
  kind: "change",
  valueColumn: "weight_change",
  valueExpr: validValueSql(
    `${TRIMMED_WEIGHT_CHANGE_SQL} NOT IN ('', 'FFF') AND (${BAN_EI_HEX_WEIGHT_CHANGE_SQL}) IS NOT NULL`,
    signedWeightChangeSql(BAN_EI_HEX_WEIGHT_CHANGE_SQL),
  ),
};

// Ban-ei has no carried-weight classes and only Ban-ei venues share its
// weight scale; flat races never mix Ban-ei rows into their bands.
const BAN_EI_WEIGHT_CLASS_COLUMNS: readonly WeightClassColumn[] = [
  BAN_EI_BODY_WEIGHT_COLUMN,
  BAN_EI_WEIGHT_CHANGE_COLUMN,
];
const WEIGHT_CLASS_COLUMNS: readonly WeightClassColumn[] = [
  BODY_WEIGHT_COLUMN,
  CARRIED_WEIGHT_COLUMN,
  WEIGHT_CHANGE_COLUMN,
];
const WEIGHT_CLASS_ROW_GROUPS: ReadonlyArray<
  [WeightClassKind, keyof ConditionWeightClassRowGroups]
> = [
  ["body", "weightRows"],
  ["carried", "carriedRows"],
  ["change", "weightChangeRows"],
];
const WEIGHT_CLASS_COLUMN_BY_KIND: ReadonlyMap<WeightClassKind, string> = new Map([
  ["body", "body_class"],
  ["carried", "carried_class"],
  ["change", "change_class"],
]);

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

const textOrEmpty = (value: unknown): string => {
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "bigint") return String(value);
  return "";
};

const roundedOrNull = (value: unknown, scale: number): number | null => {
  const parsed = optionalNumber(value);
  return parsed === null ? null : roundTo(parsed, scale);
};

const integerSelect = (expr: string, empty: string): string =>
  `try_cast(nullif(btrim(coalesce(${expr}, '')), '${empty}') AS INT)`;

export const doubleSelect = (expr: string): string =>
  `try_cast(nullif(btrim(coalesce(${expr}, '')), '') AS DOUBLE)`;

const raceTimeTenthsSelect = (expr: string): string => encodedRaceTimeTenthsSql(doubleSelect(expr));

export const trimmedNameSql = (column: string): string =>
  `coalesce(nullif(btrim(replace(coalesce(${column}, ''), chr(12288), '')), ''), '-')`;

const paddedCodeSql = (column: string): string => `lpad(btrim(coalesce(${column}, '')), 2, '0')`;

export const matchedHistoryArmSql = (input: {
  env: R2SqlCatalogConfig;
  extraJoin: string;
  extraWhere: string;
  filters: WinRateHeatmapStatsFilters;
  selectList: string;
  rateDenominator?: boolean;
  forceSameVenue?: boolean;
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
  WHERE ${similarRaceFilterSql(input.filters)}${
    // JRA confirms the 2016/2017 Arc used Chantilly during Longchamp's renovation.
    isCurrentArc(input.filters) && (input.forceSameVenue || input.filters.includeVenue)
      ? "\n    AND ra.kaisai_nen NOT IN ('2016', '2017')"
      : ""
  }
    ${cellRouterDistanceBandFilterSql(input.filters)}
    AND ${
      input.rateDenominator
        ? rateStartPredicateSql(input.filters)
        : `${finishPositionSql("se")} IS NOT NULL
    AND ${finishPositionSql("se")} > 0`
    }
    ${input.extraWhere}`;

export const buildConditionFrameStatsQuery = (
  env: R2SqlCatalogConfig,
  filters: WinRateHeatmapStatsFilters,
): string => {
  const checked = validateFilters(filters);
  const current = currentTables(checked.source);
  const arc: boolean = isCurrentArc(checked);
  const overseas = usesOverseasGate(checked);
  const frame = overseas ? "gate_mapping.gate_number" : integerSelect("se.wakuban", "00");
  const selectList = `${frame} AS frame_number,
    ${integerSelect("ra.shusso_tosu", "")} AS runner_count,
    ${rateFinishPositionSql(checked)} AS finish_position,
    ${integerSelect("se.tansho_ninkijun", "00")} AS popularity`;
  return `
WITH ${currentRaceCteSql(env, checked)},
current_frames AS (
  SELECT DISTINCT ${frame} AS frame_number
  FROM ${tableName(env, current.runnerTable)} se
  ${overseas ? overseasGateJoinSql(env, checked.source) : ""}
  WHERE ${currentRaceIdentitySql(checked, "se")}
    AND ${frame} IS NOT NULL
    AND ${frame} > 0
),
matched_history AS (
  ${unionHistorySql(
    (overseas ? [current] : historyTables(checked)).map((tables) =>
      matchedHistoryArmSql({
        env,
        extraJoin: `${overseas ? overseasGateJoinSql(env, tables.source) : ""}
          INNER JOIN current_frames cf ON cf.frame_number = ${frame}`,
        extraWhere: overseas ? "AND se.keibajo_code = cr.keibajo_code" : "",
        filters: checked,
        selectList,
        rateDenominator: true,
        forceSameVenue: true,
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
  sum(CASE WHEN ${arc ? "finish_position > 0 AND " : ""}finish_position <= 2 THEN 1 ELSE 0 END) AS quinella_count,
  sum(CASE WHEN ${arc ? "finish_position > 0 AND " : ""}finish_position <= 3 THEN 1 ELSE 0 END) AS show_count,
  avg(${arc ? "CASE WHEN finish_position > 0 THEN finish_position END" : "finish_position"}) AS average_finish,
  approx_percentile_cont(${arc ? "CASE WHEN finish_position > 0 THEN finish_position END" : "finish_position"}, 0.5) AS median_finish,
  avg(CASE WHEN popularity > 0 THEN popularity END) AS average_popularity,
  approx_percentile_cont(CASE WHEN popularity > 0 THEN popularity END, 0.5) AS median_popularity
FROM matched_history
WHERE frame_number IS NOT NULL
GROUP BY frame_number
ORDER BY frame_number`;
};

// One history scan classifies every runner by body weight, carried weight and
// weight change at once. Grouping on the class triple keeps the result small
// (at most a few hundred rows); splitConditionWeightClassRows totals each kind.
export const buildConditionWeightClassStatsQuery = (input: {
  env: R2SqlCatalogConfig;
  filters: WinRateHeatmapStatsFilters;
}): string => {
  const checked = validateFilters(input.filters);
  const banEi = isBanEiKeibajo(checked.keibajoCode);
  const columns = banEi ? BAN_EI_WEIGHT_CLASS_COLUMNS : WEIGHT_CLASS_COLUMNS;
  const arc: boolean = isCurrentArc(checked);
  const classColumns = columns.map((column) => column.classColumn).join(", ");
  return `
WITH ${currentRaceCteSql(input.env, checked)},
matched_history AS (
  ${unionHistorySql(
    historyTables(checked).map((tables) =>
      matchedHistoryArmSql({
        env: input.env,
        extraJoin: "",
        extraWhere: banEi ? BAN_EI_VENUE_ONLY_SQL : NON_BAN_EI_VENUE_SQL,
        filters: checked,
        selectList: [
          `${rateFinishPositionSql(checked)} AS finish_position`,
          ...columns.map((column) => `${column.valueExpr} AS ${column.valueColumn}`),
        ].join(",\n    "),
        rateDenominator: true,
        tables,
      }),
    ),
  )}
),
classed AS (
  SELECT
    ${columns
      .map(
        (column) =>
          `CASE WHEN ${column.valueColumn} IS NULL THEN NULL ELSE ${column.classExpr} END AS ${column.classColumn}`,
      )
      .join(",\n    ")},
    finish_position
  FROM matched_history
  WHERE ${columns.map((column) => `${column.valueColumn} IS NOT NULL`).join(" OR ")}
)
SELECT
    ${classColumns},
    count(*) AS starts,
    sum(CASE WHEN finish_position = 1 THEN 1 ELSE 0 END) AS win_count,
    sum(CASE WHEN ${arc ? "finish_position > 0 AND " : ""}finish_position <= 2 THEN 1 ELSE 0 END) AS quinella_count,
    sum(CASE WHEN ${arc ? "finish_position > 0 AND " : ""}finish_position <= 3 THEN 1 ELSE 0 END) AS show_count
  FROM classed
  GROUP BY ${classColumns}`;
};

const addWeightClassCounts = (
  current: WeightClassCounts | undefined,
  row: Record<string, unknown>,
): WeightClassCounts => ({
  quinellaCount:
    (current?.quinellaCount ?? 0) + requiredInteger(row.quinella_count, "quinella_count"),
  showCount: (current?.showCount ?? 0) + requiredInteger(row.show_count, "show_count"),
  starts: (current?.starts ?? 0) + requiredInteger(row.starts, "starts"),
  winCount: (current?.winCount ?? 0) + requiredInteger(row.win_count, "win_count"),
});

const compareClassKeys = (left: string, right: string): number => (left < right ? -1 : 1);

const totalWeightClassRows = (
  rows: ReadonlyArray<Record<string, unknown>>,
  classColumn: string,
): Record<string, unknown>[] => {
  const totals = rows.reduce((index, row) => {
    const key = row[classColumn];
    return typeof key === "string" && key !== ""
      ? index.set(key, addWeightClassCounts(index.get(key), row))
      : index;
  }, new Map<string, WeightClassCounts>());
  return [...totals.keys()].toSorted(compareClassKeys).map((key) => {
    const counts = totals.get(key)!;
    return {
      class_key: key,
      quinella_count: counts.quinellaCount,
      show_count: counts.showCount,
      starts: counts.starts,
      win_count: counts.winCount,
    };
  });
};

export const splitConditionWeightClassRows = (
  rows: ReadonlyArray<Record<string, unknown>>,
): ConditionWeightClassRowGroups => {
  const groups: ConditionWeightClassRowGroups = {
    carriedRows: [],
    weightChangeRows: [],
    weightRows: [],
  };
  return WEIGHT_CLASS_ROW_GROUPS.reduce(
    (result, [kind, group]) => ({
      ...result,
      [group]: totalWeightClassRows(rows, WEIGHT_CLASS_COLUMN_BY_KIND.get(kind)!),
    }),
    groups,
  );
};

export interface ConditionCombinedRowGroups {
  correlationTargetRows: Record<string, unknown>[];
  frameRows: Record<string, unknown>[];
  raceTimeRows: Record<string, unknown>[];
  weightClassRows: Record<string, unknown>[];
}

// Arc rates count DNF/DSQ starts and overseas frames come from gate mappings;
// those races keep the separate frame, weight, race-time and top-3 queries.
export const usesCombinedConditionStats = (filters: WinRateHeatmapStatsFilters): boolean =>
  !isCurrentArc(filters) && !usesOverseasGate(filters);

// Frame, weight-class, race-time and top-3 aggregates read the same matched
// history. One scan projects each query's extra condition (current frames,
// Ban-ei venue split, winner with a time at the exact distance, top three at
// the exact distance) as nullable columns and aggregates them with GROUPING
// SETS; splitConditionCombinedRows restores the four former result shapes.
export const buildConditionCombinedStatsQuery = (
  env: R2SqlCatalogConfig,
  filters: WinRateHeatmapStatsFilters,
): string => {
  const checked = validateFilters(filters);
  const current = currentTables(checked.source);
  const banEi = isBanEiKeibajo(checked.keibajoCode);
  const columns = banEi ? BAN_EI_WEIGHT_CLASS_COLUMNS : WEIGHT_CLASS_COLUMNS;
  const venue = banEi ? BAN_EI_VENUE_CONDITION : NON_BAN_EI_VENUE_CONDITION;
  const frame = integerSelect("se.wakuban", "00");
  const finish = finishPositionSql("se");
  const raceTime = raceTimeTenthsSelect("se.soha_time");
  const exactKyori = `${historyKyoriIntSql()} = cr.kyori_int AND cr.kyori_int IS NOT NULL`;
  const winnerWithTime = `${finish} = 1 AND ${exactKyori} AND ${raceTime} IS NOT NULL`;
  const topThree = `${finish} <= 3 AND ${exactKyori}`;
  const classColumns = columns.map((column) => column.classColumn);
  return `
WITH ${currentRaceCteSql(env, checked)},
current_frames AS (
  SELECT DISTINCT ${frame} AS frame_number
  FROM ${tableName(env, current.runnerTable)} se
  WHERE ${currentRaceIdentitySql(checked, "se")}
    AND ${frame} IS NOT NULL
    AND ${frame} > 0
),
matched_history AS (
  ${unionHistorySql(
    historyTables(checked).map((tables) =>
      matchedHistoryArmSql({
        env,
        extraJoin: `LEFT JOIN current_frames cf ON cf.frame_number = ${frame}`,
        extraWhere: "",
        filters: checked,
        selectList: [
          "cf.frame_number AS frame_number",
          `${integerSelect("ra.shusso_tosu", "")} AS runner_count`,
          `${finish} AS finish_position`,
          `${integerSelect("se.tansho_ninkijun", "00")} AS popularity`,
          ...columns.map(
            (column) =>
              `CASE WHEN ${venue} THEN ${column.valueExpr} ELSE NULL END AS ${column.valueColumn}`,
          ),
          `CASE WHEN ${winnerWithTime} THEN ${raceTime} ELSE NULL END AS win_race_time`,
          `CASE WHEN ${winnerWithTime} THEN ${doubleSelect("se.kohan_3f")} ELSE NULL END AS win_kohan_3f`,
          `CASE WHEN ${topThree} THEN 1 ELSE 0 END AS top3`,
          `CASE WHEN ${topThree} THEN nullif(${doubleSelect("se.tansho_ninkijun")}, 0) ELSE NULL END AS top3_popularity`,
          `CASE WHEN ${topThree} THEN nullif(${doubleSelect("se.tansho_odds")}, 0) / ${String(CORRELATION_ODDS_SCALE)}.0 ELSE NULL END AS top3_odds`,
        ].join(",\n    "),
        rateDenominator: true,
        tables,
      }),
    ),
  )}
),
classed AS (
  SELECT
    matched_history.*,
    ${columns
      .map(
        (column) =>
          `CASE WHEN ${column.valueColumn} IS NULL THEN NULL ELSE ${column.classExpr} END AS ${column.classColumn}`,
      )
      .join(",\n    ")}
  FROM matched_history
)
SELECT
  CASE
    WHEN grouping(frame_number) = 0 THEN 'frame'
    WHEN grouping(${classColumns[0]!}) = 0 THEN 'weights'
    ELSE 'race'
  END AS stats_set,
  frame_number,
  ${classColumns.join(",\n  ")},
  max(runner_count) AS runner_count,
  count(*) AS count,
  sum(CASE WHEN finish_position = 1 THEN 1 ELSE 0 END) AS win_count,
  sum(CASE WHEN finish_position <= 2 THEN 1 ELSE 0 END) AS quinella_count,
  sum(CASE WHEN finish_position <= 3 THEN 1 ELSE 0 END) AS show_count,
  avg(finish_position) AS average_finish,
  approx_percentile_cont(finish_position, 0.5) AS median_finish,
  avg(CASE WHEN popularity > 0 THEN popularity END) AS average_popularity,
  approx_percentile_cont(CASE WHEN popularity > 0 THEN popularity END, 0.5) AS median_popularity,
  count(win_race_time) AS race_count,
  min(win_race_time) AS fastest_race_time,
  min(win_kohan_3f) AS fastest_kohan_3f,
  avg(win_race_time) AS average_race_time,
  avg(win_kohan_3f) AS average_kohan_3f,
  approx_percentile_cont(win_race_time, 0.5) AS median_race_time,
  approx_percentile_cont(win_kohan_3f, 0.5) AS median_kohan_3f,
  sum(top3) AS top3_count,
  sum(CASE WHEN top3 = 1 AND finish_position = 1 THEN 1 ELSE 0 END) AS top3_win_count,
  avg(top3_popularity) AS top3_average_popularity,
  avg(top3_odds) AS top3_average_odds
FROM classed
GROUP BY GROUPING SETS ((frame_number), (${classColumns.join(", ")}), ())`;
};

const compareFrameRows = (left: Record<string, unknown>, right: Record<string, unknown>): number =>
  Number(left.frame_number) - Number(right.frame_number);

// Restores the frame, weight-class (combined-class triple), race-time and
// top-3 row shapes the separate queries returned.
export const splitConditionCombinedRows = (
  rows: ReadonlyArray<Record<string, unknown>>,
): ConditionCombinedRowGroups => {
  const race = rows.filter((row) => row.stats_set === "race");
  return {
    correlationTargetRows: race.map((row) => ({
      average_odds: row.top3_average_odds,
      average_popularity: row.top3_average_popularity,
      top3_count: row.top3_count,
      win_count: row.top3_win_count,
    })),
    frameRows: rows
      .filter(
        (row) =>
          row.stats_set === "frame" && row.frame_number !== null && row.frame_number !== undefined,
      )
      .toSorted(compareFrameRows),
    raceTimeRows: race,
    weightClassRows: rows
      .filter((row) => row.stats_set === "weights")
      .map((row) => ({
        body_class: row.body_class,
        carried_class: row.carried_class,
        change_class: row.change_class,
        quinella_count: row.quinella_count,
        show_count: row.show_count,
        starts: row.count,
        win_count: row.win_count,
      })),
  };
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
    concat(ra.kaisai_nen, ra.kaisai_tsukihi) AS target_race_date,
    ${paddedCodeSql("ra.keibajo_code")} AS keibajo_code,
    ${paddedCodeSql("ra.race_bango")} AS race_bango,
    coalesce(nullif(btrim(coalesce(ra.kyosomei_hondai, '')), ''), '') AS race_name,
    ${paddedCodeSql("se.wakuban")} AS wakuban,
    ${paddedCodeSql("se.umaban")} AS umaban,
    ${trimmedNameSql("se.bamei")} AS bamei,
    ${trimmedNameSql("se.kishumei_ryakusho")} AS jockey_name,
    coalesce(nullif(btrim(coalesce(se.kakutei_chakujun, '')), ''), '') AS kakutei_chakujun,
    coalesce(nullif(btrim(coalesce(se.soha_time, '')), ''), '') AS soha_time,
    coalesce(nullif(btrim(coalesce(se.tansho_ninkijun, '')), ''), '') AS popularity,
    coalesce(nullif(btrim(coalesce(se.tansho_odds, '')), ''), '') AS win_odds,
    ${integerSelect("se.tansho_ninkijun", "00")} AS popularity_number,
    ${doubleSelect("se.tansho_odds")} / 10.0 AS odds`,
        tables,
      }),
    ),
  )}
)
SELECT
  finish_position,
  target_race_date,
  keibajo_code,
  race_bango,
  race_name,
  wakuban,
  umaban,
  bamei,
  jockey_name,
  kakutei_chakujun,
  soha_time,
  popularity,
  win_odds,
  popularity_number,
  odds
FROM matched_history
ORDER BY finish_position ASC, target_race_date DESC, race_bango ASC, umaban ASC`;
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
    AND ${raceTimeTenthsSelect("se.soha_time")} IS NOT NULL
    ${exactHistoryKyoriFilterSql()}`,
        filters: checked,
        selectList: `${raceTimeTenthsSelect("se.soha_time")} AS race_time,
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
FROM matched_history
WHERE race_time IS NOT NULL`;
};

export const buildConditionTargetRacesQuery = (
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
    AND ${raceTimeTenthsSelect("se.soha_time")} IS NOT NULL
    ${exactHistoryKyoriFilterSql()}`,
        filters: checked,
        selectList: `concat(ra.kaisai_nen, ra.kaisai_tsukihi) AS target_race_date,
    ${paddedCodeSql("ra.keibajo_code")} AS keibajo_code,
    ${paddedCodeSql("ra.race_bango")} AS race_bango,
    coalesce(nullif(btrim(coalesce(ra.kyosomei_hondai, '')), ''), '') AS race_name,
    ${paddedCodeSql("se.umaban")} AS umaban,
    ${trimmedNameSql("se.bamei")} AS bamei,
    ${trimmedNameSql("se.kishumei_ryakusho")} AS jockey_name,
    ${trimmedNameSql("se.chokyoshimei_ryakusho")} AS trainer_name,
    ${trimmedNameSql("se.banushimei")} AS owner_name,
    ${doubleSelect("se.soha_time")} AS race_time,
    ${doubleSelect("se.kohan_3f")} AS kohan_3f,
    coalesce(nullif(btrim(coalesce(ra.kyori, '')), ''), '') AS kyori,
    coalesce(nullif(btrim(coalesce(se.tansho_ninkijun, '')), ''), '') AS popularity`,
        tables,
      }),
    ),
  )}
)
SELECT
  target_race_date,
  keibajo_code,
  race_bango,
  race_name,
  umaban,
  bamei,
  jockey_name,
  trainer_name,
  owner_name,
  race_time,
  kohan_3f,
  kyori,
  popularity
FROM matched_history
ORDER BY target_race_date DESC, race_bango ASC
LIMIT ${String(TARGET_RACE_LIMIT)}`;
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

const compareFinishDetailOrder = (
  left: ConditionFinishPositionDetail,
  right: ConditionFinishPositionDetail,
): number => {
  if (left.date !== right.date) return left.date < right.date ? 1 : -1;
  if (left.raceNumber !== right.raceNumber) return left.raceNumber < right.raceNumber ? -1 : 1;
  if (left.horseNumber === right.horseNumber) return 0;
  return left.horseNumber < right.horseNumber ? -1 : 1;
};

const compareFinishPosition = (left: number, right: number): number => left - right;

const averageOf = (values: readonly number[]): number | null => {
  if (values.length === 0) return null;
  const total = values.reduce((sum, value) => sum + value, 0);
  return roundTo(total / values.length, RATE_SCALE);
};

const medianOf = (values: readonly number[]): number | null => {
  if (values.length === 0) return null;
  const sorted = [...values].toSorted((left, right) => left - right);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) {
    const value = sorted[mid];
    return value === undefined ? null : roundTo(value, RATE_SCALE);
  }
  const low = sorted[mid - 1];
  const high = sorted[mid];
  if (low === undefined || high === undefined) return null;
  return roundTo((low + high) / 2, RATE_SCALE);
};

const parseFinishHorseRow = (
  raw: Record<string, unknown>,
): {
  detail: ConditionFinishPositionDetail;
  finishPosition: number;
  odds: number | null;
  popularity: number | null;
} | null => {
  const finishPosition = optionalNumber(raw.finish_position);
  if (finishPosition === null || !Number.isInteger(finishPosition) || finishPosition <= 0) {
    return null;
  }
  const popularityNumber = optionalNumber(raw.popularity_number);
  const odds = optionalNumber(raw.odds);
  return {
    detail: {
      date: requiredString(raw.target_race_date, "target_race_date"),
      frameNumber: textOrEmpty(raw.wakuban),
      horseName: requiredString(raw.bamei, "bamei"),
      horseNumber: requiredString(raw.umaban, "umaban"),
      jockeyName: requiredString(raw.jockey_name, "jockey_name"),
      keibajoCode: requiredString(raw.keibajo_code, "keibajo_code"),
      popularity: textOrEmpty(raw.popularity),
      raceName: textOrEmpty(raw.race_name),
      raceNumber: requiredString(raw.race_bango, "race_bango"),
      raceTime: textOrEmpty(raw.soha_time),
      rank: textOrEmpty(raw.kakutei_chakujun),
      winOdds: textOrEmpty(raw.win_odds),
    },
    finishPosition,
    odds: odds !== null && odds > 0 ? odds : null,
    popularity: popularityNumber !== null && popularityNumber > 0 ? popularityNumber : null,
  };
};

const finishStatsFromHorses = (
  horses: ReadonlyArray<NonNullable<ReturnType<typeof parseFinishHorseRow>>>,
): ConditionFinishPositionStatsRow[] => {
  const grouped = horses.reduce<
    Map<number, Array<NonNullable<ReturnType<typeof parseFinishHorseRow>>>>
  >((groups, horse) => {
    const existing = groups.get(horse.finishPosition);
    if (existing === undefined) {
      return groups.set(horse.finishPosition, [horse]);
    }
    return groups.set(horse.finishPosition, [...existing, horse]);
  }, new Map());
  return [...grouped.keys()].toSorted(compareFinishPosition).flatMap((finishPosition) => {
    const group = grouped.get(finishPosition);
    if (group === undefined) return [];
    const details = group
      .map((horse) => horse.detail)
      .toSorted(compareFinishDetailOrder)
      .slice(0, FINISH_DETAIL_LIMIT);
    return [
      {
        averageOdds: averageOf(group.flatMap((horse) => (horse.odds === null ? [] : [horse.odds]))),
        averagePopularity: averageOf(
          group.flatMap((horse) => (horse.popularity === null ? [] : [horse.popularity])),
        ),
        count: group.length,
        details,
        finishPosition,
        medianOdds: medianOf(group.flatMap((horse) => (horse.odds === null ? [] : [horse.odds]))),
        medianPopularity: medianOf(
          group.flatMap((horse) => (horse.popularity === null ? [] : [horse.popularity])),
        ),
      },
    ];
  });
};

export const aggregateFinishPositionStats = (
  rawRows: ReadonlyArray<Record<string, unknown>>,
): ConditionFinishPositionStatsRow[] =>
  finishStatsFromHorses(
    rawRows.flatMap((raw) => {
      const parsed = parseFinishHorseRow(raw);
      return parsed === null ? [] : [parsed];
    }),
  );

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
  targetRaces: [],
});

export const normaliseTargetRaceRow = (raw: Record<string, unknown>): ConditionTargetRace => ({
  date: requiredString(raw.target_race_date, "target_race_date"),
  horseName: requiredString(raw.bamei, "bamei"),
  horseNumber: requiredString(raw.umaban, "umaban"),
  jockeyName: requiredString(raw.jockey_name, "jockey_name"),
  keibajoCode: requiredString(raw.keibajo_code, "keibajo_code"),
  kohan3f: textOrEmpty(raw.kohan_3f),
  kyori: textOrEmpty(raw.kyori),
  ownerName: requiredString(raw.owner_name, "owner_name"),
  popularity: textOrEmpty(raw.popularity),
  raceName: textOrEmpty(raw.race_name),
  raceNumber: requiredString(raw.race_bango, "race_bango"),
  raceTime: textOrEmpty(raw.race_time),
  trainerName: requiredString(raw.trainer_name, "trainer_name"),
});

export const normaliseRaceTimeStats = (
  raw: Record<string, unknown> | undefined,
  targetRaces: ConditionTargetRace[],
  correlationRows: ConditionCorrelationRow[],
): ConditionRaceTimeStats => {
  if (raw === undefined) return { ...emptyRaceTimeStats(), correlationRows, targetRaces };
  return {
    averageKohan3f: roundedOrNull(raw.average_kohan_3f, RATE_SCALE),
    averageRaceTime: roundedOrNull(raw.average_race_time, RATE_SCALE),
    correlationRows,
    fastestDetail: null,
    fastestKohan3f: roundedOrNull(raw.fastest_kohan_3f, RATE_SCALE),
    fastestRaceTime: roundedOrNull(raw.fastest_race_time, RATE_SCALE),
    medianKohan3f: roundedOrNull(raw.median_kohan_3f, RATE_SCALE),
    medianRaceTime: roundedOrNull(raw.median_race_time, RATE_SCALE),
    raceCount: requiredInteger(raw.race_count, "race_count"),
    targetRaces,
  };
};

export const normaliseConditionHistoryStatsPayload = (input: {
  carriedRows: ReadonlyArray<Record<string, unknown>>;
  correlationRows: ConditionCorrelationRow[];
  finishRows: ReadonlyArray<Record<string, unknown>>;
  frameRows: ReadonlyArray<Record<string, unknown>>;
  raceTimeRows: ReadonlyArray<Record<string, unknown>>;
  targetRaceRows: ReadonlyArray<Record<string, unknown>>;
  weightChangeRows: ReadonlyArray<Record<string, unknown>>;
  weightRows: ReadonlyArray<Record<string, unknown>>;
}): ConditionHistoryStatsPayload => ({
  carriedWeightClassStats: input.carriedRows.map(normaliseWeightClassRow),
  finishPositionStats: aggregateFinishPositionStats(input.finishRows),
  frameStats: scoredFrameRows(input.frameRows.map(normaliseFrameRow)),
  raceTimeStats: normaliseRaceTimeStats(
    input.raceTimeRows[0],
    input.targetRaceRows.map(normaliseTargetRaceRow),
    input.correlationRows,
  ),
  weightChangeClassStats: input.weightChangeRows.map(normaliseWeightClassRow),
  weightClassStats: input.weightRows.map(normaliseWeightClassRow),
});
