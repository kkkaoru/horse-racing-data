// Run with bun (bunx vitest).

import type { R2SqlCatalogConfig, RaceTrainingFilters, RaceTrainingRow } from "./types";

const IDENTIFIER_PATTERN: RegExp = /^[A-Za-z_][A-Za-z0-9_]*$/u;
const DATE_PATTERN: RegExp = /^\d{8}$/u;
const CODE_PATTERN: RegExp = /^\d{2}$/u;
const LOOKBACK_DAYS: number = 14;
const MILLISECONDS_PER_DAY: number = 86_400_000;

const WORKOUT_VALUE_COLUMN_NAMES: ReadonlyArray<string> = [
  "time_gokei_10f",
  "lap_time_10f",
  "time_gokei_9f",
  "lap_time_9f",
  "time_gokei_8f",
  "lap_time_8f",
  "time_gokei_7f",
  "lap_time_7f",
  "time_gokei_6f",
  "lap_time_6f",
  "time_gokei_5f",
  "lap_time_5f",
  "time_gokei_4f",
  "lap_time_4f",
  "time_gokei_3f",
  "lap_time_3f",
  "time_gokei_2f",
  "lap_time_2f",
  "lap_time_1f",
];
const WORKOUT_VALUE_COLUMNS: string = WORKOUT_VALUE_COLUMN_NAMES.join(", ");
const NETKEIBA_WORKOUT_VALUE_COLUMNS: string = WORKOUT_VALUE_COLUMN_NAMES.map(
  (column) => `n.${column}`,
).join(", ");
const WORKOUT_ROW_COLUMNS: string = `
    umaban, bamei, current_jockey_name, trainer_name, ketto_toroku_bango,
    training_type, tracen_kubun, chokyo_nengappi, chokyo_jikoku, course, babamawari,
    ${WORKOUT_VALUE_COLUMNS},
    training_rider_name, premium_evaluation_text, premium_evaluation_grade,
    premium_comment_text, premium_workout_index, training_data_source`;
const TRAINING_RESPONSE_COLUMNS: string = `
  umaban, bamei, current_jockey_name, trainer_name,
  training_type, tracen_kubun, chokyo_nengappi, chokyo_jikoku, course, babamawari,
  ${WORKOUT_VALUE_COLUMNS},
  training_rider_name, premium_evaluation_text, premium_evaluation_grade,
  premium_comment_text, premium_workout_index, training_data_source`;

const requireIdentifier = (value: string, label: string): string => {
  if (!IDENTIFIER_PATTERN.test(value)) {
    throw new Error(`${label} must be an unquoted SQL identifier`);
  }
  return value;
};

const requireDate = (value: string): string => {
  if (!DATE_PATTERN.test(value)) throw new Error("date must match YYYYMMDD");
  return value;
};

const requireCode = (value: string, label: string): string => {
  if (!CODE_PATTERN.test(value)) throw new Error(`${label} must contain two digits`);
  return value;
};

const compactUtcDate = (timestamp: number): string =>
  new Date(timestamp).toISOString().slice(0, 10).replaceAll("-", "");

const workoutWindowStart = (date: string): string => {
  const checked = requireDate(date);
  const timestamp = Date.UTC(
    Number(checked.slice(0, 4)),
    Number(checked.slice(4, 6)) - 1,
    Number(checked.slice(6, 8)),
  );
  if (compactUtcDate(timestamp) !== checked) throw new Error("date must be a valid calendar date");
  return compactUtcDate(timestamp - LOOKBACK_DAYS * MILLISECONDS_PER_DAY);
};

const rawTableName = (env: R2SqlCatalogConfig, table: string): string =>
  `${requireIdentifier(env.R2_SQL_NAMESPACE, "R2_SQL_NAMESPACE")}.${requireIdentifier(table, "table")}`;

const officialWorkoutSelect = (
  table: string,
  trainingType: string,
  extendedColumns: boolean,
): string => `SELECT
    r.umaban, r.bamei, r.current_jockey_name, r.trainer_name,
    r.ketto_toroku_bango,
    '${trainingType}' AS training_type,
    w.tracen_kubun, w.chokyo_nengappi, w.chokyo_jikoku,
    ${extendedColumns ? "w.course" : "NULL"} AS course,
    ${extendedColumns ? "w.babamawari" : "NULL"} AS babamawari,
    ${extendedColumns ? "w.time_gokei_10f" : "NULL"} AS time_gokei_10f,
    ${extendedColumns ? "w.lap_time_10f" : "NULL"} AS lap_time_10f,
    ${extendedColumns ? "w.time_gokei_9f" : "NULL"} AS time_gokei_9f,
    ${extendedColumns ? "w.lap_time_9f" : "NULL"} AS lap_time_9f,
    ${extendedColumns ? "w.time_gokei_8f" : "NULL"} AS time_gokei_8f,
    ${extendedColumns ? "w.lap_time_8f" : "NULL"} AS lap_time_8f,
    ${extendedColumns ? "w.time_gokei_7f" : "NULL"} AS time_gokei_7f,
    ${extendedColumns ? "w.lap_time_7f" : "NULL"} AS lap_time_7f,
    ${extendedColumns ? "w.time_gokei_6f" : "NULL"} AS time_gokei_6f,
    ${extendedColumns ? "w.lap_time_6f" : "NULL"} AS lap_time_6f,
    ${extendedColumns ? "w.time_gokei_5f" : "NULL"} AS time_gokei_5f,
    ${extendedColumns ? "w.lap_time_5f" : "NULL"} AS lap_time_5f,
    w.time_gokei_4f, w.lap_time_4f,
    w.time_gokei_3f, w.lap_time_3f,
    w.time_gokei_2f, w.lap_time_2f,
    w.lap_time_1f,
    NULL AS training_rider_name,
    NULL AS premium_evaluation_text,
    NULL AS premium_evaluation_grade,
    NULL AS premium_comment_text,
    NULL AS premium_workout_index,
    'jra' AS training_data_source,
    0 AS source_priority
  FROM runners r
  INNER JOIN ${table} w ON w.ketto_toroku_bango = r.ketto_toroku_bango
  CROSS JOIN workout_window ww
  WHERE w.chokyo_nengappi BETWEEN ww.start_date AND ww.end_date`;

export const buildRaceTrainingsQuery = (
  env: R2SqlCatalogConfig,
  filters: RaceTrainingFilters,
): string => {
  const date = requireDate(filters.date);
  const keibajoCode = requireCode(filters.keibajoCode, "keibajoCode");
  const raceBango = requireCode(filters.raceBango, "raceBango");
  const startDate = workoutWindowStart(date);
  const kaisaiNen = date.slice(0, 4);
  const kaisaiTsukihi = date.slice(4, 8);
  const runnersTable = rawTableName(env, "jvd_se");
  const hillTable = rawTableName(env, "jvd_hc");
  const woodTable = rawTableName(env, "jvd_wc");
  const netkeibaTable = rawTableName(env, "netkeiba_training_workouts");
  const hillSelect = officialWorkoutSelect(hillTable, "坂路", false);
  const woodSelect = officialWorkoutSelect(woodTable, "ウッド", true);

  return `WITH runners AS (
  SELECT
    lpad(umaban, 2, '0') AS umaban,
    bamei,
    kishumei_ryakusho AS current_jockey_name,
    chokyoshimei_ryakusho AS trainer_name,
    ketto_toroku_bango
  FROM ${runnersTable}
  WHERE kaisai_nen = '${kaisaiNen}'
    AND kaisai_tsukihi = '${kaisaiTsukihi}'
    AND keibajo_code = '${keibajoCode}'
    AND race_bango = '${raceBango}'
),
workout_window AS (
  SELECT '${startDate}' AS start_date, '${date}' AS end_date
),
official_workouts AS (
  ${hillSelect}
  UNION ALL
  ${woodSelect}
),
netkeiba_workouts AS (
  SELECT
    r.umaban,
    coalesce(nullif(n.bamei, ''), r.bamei) AS bamei,
    r.current_jockey_name,
    r.trainer_name,
    r.ketto_toroku_bango,
    coalesce(nullif(n.training_type, ''), '-') AS training_type,
    n.tracen_kubun, n.chokyo_nengappi, n.chokyo_jikoku,
    n.course, n.babamawari,
    ${NETKEIBA_WORKOUT_VALUE_COLUMNS},
    n.rider_name AS training_rider_name,
    n.evaluation_text AS premium_evaluation_text,
    n.evaluation_grade AS premium_evaluation_grade,
    n.comment_text AS premium_comment_text,
    try_cast(n.workout_index AS INT) AS premium_workout_index,
    'netkeiba' AS training_data_source,
    1 AS source_priority
  FROM ${netkeibaTable} n
  INNER JOIN runners r ON r.ketto_toroku_bango = n.ketto_toroku_bango
  WHERE n.kaisai_nen = '${kaisaiNen}'
    AND n.kaisai_tsukihi = '${kaisaiTsukihi}'
    AND n.keibajo_code = '${keibajoCode}'
    AND n.race_bango = '${raceBango}'
),
combined_workouts AS (
  SELECT${WORKOUT_ROW_COLUMNS}, source_priority FROM official_workouts
  UNION ALL
  SELECT${WORKOUT_ROW_COLUMNS}, source_priority FROM netkeiba_workouts
),
deduplicated_workouts AS (
  SELECT${WORKOUT_ROW_COLUMNS}
  FROM (
    SELECT${WORKOUT_ROW_COLUMNS}, source_priority, row_number() OVER (
      PARTITION BY
        ketto_toroku_bango, training_type, tracen_kubun, chokyo_nengappi, chokyo_jikoku,
        course, babamawari, ${WORKOUT_VALUE_COLUMNS}
      ORDER BY source_priority
    ) AS signature_rank
    FROM combined_workouts
  ) ranked
  WHERE signature_rank = 1
),
placeholder_rows AS (
  SELECT
    r.umaban, r.bamei, r.current_jockey_name, r.trainer_name,
    r.ketto_toroku_bango,
    '-' AS training_type,
    NULL AS tracen_kubun, '' AS chokyo_nengappi, '' AS chokyo_jikoku,
    NULL AS course, NULL AS babamawari,
    NULL AS time_gokei_10f, NULL AS lap_time_10f,
    NULL AS time_gokei_9f, NULL AS lap_time_9f,
    NULL AS time_gokei_8f, NULL AS lap_time_8f,
    NULL AS time_gokei_7f, NULL AS lap_time_7f,
    NULL AS time_gokei_6f, NULL AS lap_time_6f,
    NULL AS time_gokei_5f, NULL AS lap_time_5f,
    NULL AS time_gokei_4f, NULL AS lap_time_4f,
    NULL AS time_gokei_3f, NULL AS lap_time_3f,
    NULL AS time_gokei_2f, NULL AS lap_time_2f,
    NULL AS lap_time_1f,
    NULL AS training_rider_name,
    NULL AS premium_evaluation_text,
    NULL AS premium_evaluation_grade,
    NULL AS premium_comment_text,
    NULL AS premium_workout_index,
    'jra' AS training_data_source
  FROM runners r
  WHERE NOT EXISTS (
    SELECT 1 FROM deduplicated_workouts w
    WHERE w.ketto_toroku_bango = r.ketto_toroku_bango
  )
),
all_rows AS (
  SELECT${WORKOUT_ROW_COLUMNS} FROM deduplicated_workouts
  UNION ALL
  SELECT${WORKOUT_ROW_COLUMNS} FROM placeholder_rows
)
SELECT${TRAINING_RESPONSE_COLUMNS}
FROM all_rows
ORDER BY try_cast(umaban AS INT), chokyo_nengappi DESC, chokyo_jikoku DESC, training_type`;
};

const stringOrNull = (value: unknown): string | null => {
  if (value === null || value === undefined) return null;
  return typeof value === "string" ? value : String(value);
};

const requiredString = (value: unknown, field: string): string => {
  const normalized = stringOrNull(value);
  if (normalized === null) throw new Error(`R2 SQL row is missing ${field}`);
  return normalized;
};

const optionalNumber = (value: unknown): number | undefined => {
  if (value === null || value === undefined || value === "") return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
};

const trainingSource = (value: unknown): "jra" | "netkeiba" => {
  if (value === "jra" || value === "netkeiba") return value;
  throw new Error(`R2 SQL row has invalid training_data_source: ${String(value)}`);
};

export const normaliseRaceTrainingRow = (raw: Record<string, unknown>): RaceTrainingRow => {
  const premiumWorkoutIndex = optionalNumber(raw.premium_workout_index);
  const row: RaceTrainingRow = {
    babamawari: stringOrNull(raw.babamawari),
    bamei: stringOrNull(raw.bamei),
    chokyoJikoku: requiredString(raw.chokyo_jikoku, "chokyo_jikoku"),
    chokyoNengappi: requiredString(raw.chokyo_nengappi, "chokyo_nengappi"),
    course: stringOrNull(raw.course),
    currentJockeyName: stringOrNull(raw.current_jockey_name),
    lapTime10f: stringOrNull(raw.lap_time_10f),
    lapTime1f: stringOrNull(raw.lap_time_1f),
    lapTime2f: stringOrNull(raw.lap_time_2f),
    lapTime3f: stringOrNull(raw.lap_time_3f),
    lapTime4f: stringOrNull(raw.lap_time_4f),
    lapTime5f: stringOrNull(raw.lap_time_5f),
    lapTime6f: stringOrNull(raw.lap_time_6f),
    lapTime7f: stringOrNull(raw.lap_time_7f),
    lapTime8f: stringOrNull(raw.lap_time_8f),
    lapTime9f: stringOrNull(raw.lap_time_9f),
    premiumCommentText: stringOrNull(raw.premium_comment_text),
    premiumEvaluationGrade: stringOrNull(raw.premium_evaluation_grade),
    premiumEvaluationText: stringOrNull(raw.premium_evaluation_text),
    timeGokei10f: stringOrNull(raw.time_gokei_10f),
    timeGokei2f: stringOrNull(raw.time_gokei_2f),
    timeGokei3f: stringOrNull(raw.time_gokei_3f),
    timeGokei4f: stringOrNull(raw.time_gokei_4f),
    timeGokei5f: stringOrNull(raw.time_gokei_5f),
    timeGokei6f: stringOrNull(raw.time_gokei_6f),
    timeGokei7f: stringOrNull(raw.time_gokei_7f),
    timeGokei8f: stringOrNull(raw.time_gokei_8f),
    timeGokei9f: stringOrNull(raw.time_gokei_9f),
    tracenKubun: stringOrNull(raw.tracen_kubun),
    trainerName: stringOrNull(raw.trainer_name),
    trainingDataSource: trainingSource(raw.training_data_source),
    trainingRiderName: stringOrNull(raw.training_rider_name),
    trainingType: requiredString(raw.training_type, "training_type"),
    umaban: stringOrNull(raw.umaban),
  };
  return premiumWorkoutIndex === undefined ? row : { ...row, premiumWorkoutIndex };
};
