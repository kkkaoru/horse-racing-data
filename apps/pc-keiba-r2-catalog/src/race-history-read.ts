// Runs with bun; read-only Catalog history projection for the time-score reader.
export type RaceHistorySource = "jra" | "nar" | "overseas";

export interface RaceHistoryReadInput {
  namespace: string;
  source: RaceHistorySource;
  horseIds: readonly string[];
  beforeDate: string;
  minDate: string | null;
  limit: number;
}

export interface RaceHistoryOverseasRow {
  sourceHorseId: string;
  raceDate: string;
  distanceMetres: string | null;
}
export interface RaceHistoryReadOptions {
  input: RaceHistoryReadInput;
  query: (sql: string) => Promise<unknown[]>;
}

const COMPACT_DATE: RegExp = /^(\d{4})(\d{2})(\d{2})$/u;

// The overseas history table stores race_date as a YYYY-MM-DD string, while
// callers pass the compact YYYYMMDD bound used everywhere else.
const toDashedDate = (date: string): string => {
  const match: RegExpMatchArray | null = COMPACT_DATE.exec(date);
  if (match === null) throw new Error("Invalid race history input");
  return `${match[1]}-${match[2]}-${match[3]}`;
};

const buildOverseasHistorySql = (input: RaceHistoryReadInput, ids: string): string => {
  const before: string = toDashedDate(input.beforeDate);
  const lower: string =
    input.minDate === null ? "" : ` AND race_date >= '${toDashedDate(input.minDate)}'`;
  return `SELECT source_horse_id, race_date, distance_metres
FROM ${input.namespace}.oversea_horse_race_history
WHERE source = 'netkeiba'
  AND source_horse_id IN (${ids})
  AND race_date < '${before}'${lower}
ORDER BY source_horse_id ASC, race_date DESC
LIMIT ${input.limit + 1}`;
};

const parseOverseasRow = (
  value: unknown,
  requested: ReadonlySet<string>,
): RaceHistoryOverseasRow => {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new Error("Invalid overseas history row");
  if (Object.keys(value).length !== OVERSEAS_COLUMN_COUNT)
    throw new Error("Missing or invalid overseas history field");
  const sourceHorseId: unknown = Reflect.get(value, "source_horse_id");
  const raceDate: unknown = Reflect.get(value, "race_date");
  const distanceMetres: unknown = Reflect.get(value, "distance_metres");
  if (typeof sourceHorseId !== "string" || !requested.has(sourceHorseId))
    throw new Error("Invalid overseas history horse identity");
  if (typeof raceDate !== "string" || !/^\d{4}-\d{2}-\d{2}$/u.test(raceDate))
    throw new Error("Invalid overseas history race date");
  if (
    distanceMetres !== null &&
    typeof distanceMetres !== "string" &&
    typeof distanceMetres !== "number"
  )
    throw new Error("Missing or invalid overseas history field");
  return {
    sourceHorseId,
    raceDate,
    distanceMetres: distanceMetres === null ? null : String(distanceMetres),
  };
};

export const readOverseasRaceHistory = async (
  options: RaceHistoryReadOptions,
): Promise<RaceHistoryOverseasRow[]> => {
  if (options.input.source !== "overseas") throw new Error("Invalid race history input");
  const rows: unknown[] = await options.query(buildRaceHistoryReadSql(options.input));
  if (rows.length > options.input.limit) throw new Error("Too many race history rows");
  const requested: ReadonlySet<string> = new Set(options.input.horseIds);
  return rows.map((row) => parseOverseasRow(row, requested));
};

// Raw mirror values only: the viewer keeps its own string parsing
// (soha_time is re-encoded as tenths there), so no conversion happens here.
const COLUMNS: readonly string[] = [
  "ketto_toroku_bango",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "umaban",
  "kyori",
  "soha_time",
  "kohan_3f",
  "bataiju",
  "futan_juryo",
  "time_sa",
  "kakutei_chakujun",
];
const IDENTIFIER: RegExp = /^[A-Za-z_][A-Za-z0-9_]*$/u;
const DATE: RegExp = /^\d{8}$/u;
const HORSE_ID: RegExp = /^\d{10}$/u;
const MAX_HORSE_IDS: number = 40;
const MAX_LIMIT: number = 4000;
const COLUMN_COUNT: number = COLUMNS.length;
const OVERSEAS_COLUMN_COUNT: number = 3;

const validDate = (date: string): boolean => {
  if (!DATE.test(date)) return false;
  const iso: string = `${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}`;
  const parsed: Date = new Date(`${iso}T00:00:00.000Z`);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === iso;
};

export const buildRaceHistoryReadSql = (input: RaceHistoryReadInput): string => {
  if (
    !IDENTIFIER.test(input.namespace) ||
    (input.source !== "jra" && input.source !== "nar" && input.source !== "overseas") ||
    input.horseIds.length === 0 ||
    input.horseIds.length > MAX_HORSE_IDS ||
    !input.horseIds.every((id) => HORSE_ID.test(id)) ||
    new Set(input.horseIds).size !== input.horseIds.length ||
    !validDate(input.beforeDate) ||
    (input.minDate !== null && !validDate(input.minDate)) ||
    !Number.isSafeInteger(input.limit) ||
    input.limit <= 0 ||
    input.limit > MAX_LIMIT
  )
    throw new Error("Invalid race history input");
  const ids: string = input.horseIds.map((id) => `'${id}'`).join(", ");
  if (input.source === "overseas") return buildOverseasHistorySql(input, ids);
  const window: string =
    input.minDate === null
      ? `concat(ra.kaisai_nen, ra.kaisai_tsukihi) < '${input.beforeDate}'`
      : `concat(ra.kaisai_nen, ra.kaisai_tsukihi) < '${input.beforeDate}' AND concat(ra.kaisai_nen, ra.kaisai_tsukihi) >= '${input.minDate}'`;
  const runnerTable: string = input.source === "jra" ? "jvd_se" : "nvd_se";
  const raceTable: string = input.source === "jra" ? "jvd_ra" : "nvd_ra";
  return `SELECT ${COLUMNS.map((column) => `se.${column}`).join(", ")}
FROM ${input.namespace}.${runnerTable} se
INNER JOIN ${input.namespace}.${raceTable} ra
  ON ra.kaisai_nen = se.kaisai_nen
  AND ra.kaisai_tsukihi = se.kaisai_tsukihi
  AND ra.keibajo_code = se.keibajo_code
  AND ra.race_bango = se.race_bango
WHERE se.ketto_toroku_bango IN (${ids})
  AND ${window}
ORDER BY se.ketto_toroku_bango ASC, concat(ra.kaisai_nen, ra.kaisai_tsukihi) DESC, se.umaban ASC
LIMIT ${input.limit + 1}`;
};

const parseHistoryRow = (value: unknown): Record<string, string | null> => {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new Error("Invalid race history row");
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new Error("Invalid race history row");
  if (Object.keys(value).length !== COLUMN_COUNT)
    throw new Error("Missing or invalid race history field");
  const row: Record<string, string | null> = {};
  for (const column of COLUMNS) {
    const field: unknown = Reflect.get(value, column);
    if (field !== null && typeof field !== "string")
      throw new Error("Missing or invalid race history field");
    row[column] = field;
  }
  const horseId: string | null | undefined = row.ketto_toroku_bango;
  if (horseId === null || horseId === undefined || !HORSE_ID.test(horseId))
    throw new Error("Invalid race history horse identity");
  return row;
};

export const readRaceHistory = async (
  options: RaceHistoryReadOptions,
): Promise<Record<string, string | null>[]> => {
  if (options.input.source === "overseas") throw new Error("Invalid race history input");
  const rows: unknown[] = await options.query(buildRaceHistoryReadSql(options.input));
  if (rows.length > options.input.limit) throw new Error("Too many race history rows");
  const history: Record<string, string | null>[] = rows.map((row) => parseHistoryRow(row));
  const requested: Set<string> = new Set(options.input.horseIds);
  if (history.some((row) => !requested.has(row.ketto_toroku_bango ?? "")))
    throw new Error("Race history horse identity mismatch");
  return history;
};
