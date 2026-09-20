// Runs with bun; complete single-day race listing without jockey-name enrichment.
export interface RaceDayListInput {
  namespace: string;
  date: string;
}
export interface RaceDayListReader {
  input: RaceDayListInput;
  query: (sql: string) => Promise<unknown[]>;
}
export interface RaceDayListRow {
  source: "jra" | "nar";
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  kyosomeiHondai: string | null;
  kyosomeiFukudai: string | null;
  gradeCode: string | null;
  kyosoShubetsuCode: string | null;
  kyosoKigoCode: string | null;
  juryoShubetsuCode: string | null;
  jockeyNames: string[];
  kyosoJokenCode: string | null;
  kyosoJokenMeisho: string | null;
  kyori: string | null;
  trackCode: string | null;
  hassoJikoku: string | null;
  shussoTosu: string | null;
}
const IDENTIFIER: RegExp = /^[A-Za-z_][A-Za-z0-9_]*$/u;
const DATE: RegExp = /^[1-9]\d{7}$/u;
const VENUE: RegExp = /^[0-9A-Z]{2}$/u;
const RACE: RegExp = /^\d{2}$/u;
const MAX_DAY_RACES: number = 4096;
const COLUMNS: readonly string[] = [
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "kyosomei_hondai",
  "kyosomei_fukudai",
  "grade_code",
  "kyoso_shubetsu_code",
  "kyoso_kigo_code",
  "juryo_shubetsu_code",
  "kyoso_joken_code",
  "kyoso_joken_meisho",
  "kyori",
  "track_code",
  "hasso_jikoku",
  "shusso_tosu",
];

const validDate = (date: string): boolean => {
  if (!DATE.test(date)) return false;
  const iso: string = `${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6)}`;
  const parsed: Date = new Date(`${iso}T00:00:00.000Z`);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === iso;
};

export const validateRaceDayListInput = (input: RaceDayListInput): void => {
  if (!IDENTIFIER.test(input.namespace) || !validDate(input.date)) {
    throw new Error("Invalid race day list input");
  }
};

export const buildRaceDayListReadSql = (input: RaceDayListInput): string => {
  validateRaceDayListInput(input);
  const predicate: string = `kaisai_nen = '${input.date.slice(0, 4)}' AND kaisai_tsukihi = '${input.date.slice(4)}'`;
  const columns: string = COLUMNS.join(", ");
  // R2 SQL's distributed planner can double-scan the NAR partition when JRA and
  // NAR are combined with UNION ALL, returning every NAR race twice (the same
  // partition fault documented in worker.ts for /race-keys). UNION dedupes the
  // identical rows in the engine so the duplicate-identity guard stays strict.
  return `SELECT 'jra' AS source, ${columns}\nFROM ${input.namespace}.jvd_ra WHERE ${predicate}\nUNION\nSELECT 'nar' AS source, ${columns}\nFROM ${input.namespace}.nvd_ra WHERE ${predicate}\nORDER BY hasso_jikoku ASC NULLS LAST, keibajo_code ASC, race_bango ASC, source ASC`;
};

const nullableString = (row: object, column: string): string | null => {
  const value: unknown = Reflect.get(row, column);
  if (typeof value !== "string" && value !== null) throw new Error("Invalid race day list field");
  return value;
};

const parseRow = (value: unknown, date: string): RaceDayListRow => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("Invalid race day list row");
  }
  const source: unknown = Reflect.get(value, "source");
  const year: unknown = Reflect.get(value, "kaisai_nen");
  const monthDay: unknown = Reflect.get(value, "kaisai_tsukihi");
  const venue: unknown = Reflect.get(value, "keibajo_code");
  const race: unknown = Reflect.get(value, "race_bango");
  if (
    (source !== "jra" && source !== "nar") ||
    year !== date.slice(0, 4) ||
    monthDay !== date.slice(4) ||
    typeof venue !== "string" ||
    !VENUE.test(venue) ||
    typeof race !== "string" ||
    !RACE.test(race)
  )
    throw new Error("Invalid race day list identity");
  return {
    source,
    kaisaiNen: year,
    kaisaiTsukihi: monthDay,
    keibajoCode: venue,
    raceBango: race,
    kyosomeiHondai: nullableString(value, "kyosomei_hondai"),
    kyosomeiFukudai: nullableString(value, "kyosomei_fukudai"),
    gradeCode: nullableString(value, "grade_code"),
    kyosoShubetsuCode: nullableString(value, "kyoso_shubetsu_code"),
    kyosoKigoCode: nullableString(value, "kyoso_kigo_code"),
    juryoShubetsuCode: nullableString(value, "juryo_shubetsu_code"),
    jockeyNames: [],
    kyosoJokenCode: nullableString(value, "kyoso_joken_code"),
    kyosoJokenMeisho: nullableString(value, "kyoso_joken_meisho"),
    kyori: nullableString(value, "kyori"),
    trackCode: nullableString(value, "track_code"),
    hassoJikoku: nullableString(value, "hasso_jikoku"),
    shussoTosu: nullableString(value, "shusso_tosu"),
  };
};

const compareText = (left: string, right: string): number => {
  if (left === right) return 0;
  return left < right ? -1 : 1;
};
const compareStart = (left: string | null, right: string | null): number => {
  if (left === right) return 0;
  if (left === null) return 1;
  if (right === null) return -1;
  return compareText(left, right);
};
const compareRows = (left: RaceDayListRow, right: RaceDayListRow): number =>
  compareStart(left.hassoJikoku, right.hassoJikoku) ||
  compareText(left.keibajoCode, right.keibajoCode) ||
  compareText(left.raceBango, right.raceBango) ||
  compareText(left.source, right.source);
const identity = (row: RaceDayListRow): string =>
  `${row.source}/${row.keibajoCode}/${row.raceBango}`;

export const readRaceDayList = async (reader: RaceDayListReader): Promise<RaceDayListRow[]> => {
  const rows: unknown[] = await reader.query(buildRaceDayListReadSql(reader.input));
  if (rows.length > MAX_DAY_RACES) throw new Error("Race day list exceeds row limit");
  const races: RaceDayListRow[] = rows.map((row) => parseRow(row, reader.input.date));
  if (new Set(races.map(identity)).size !== races.length) {
    throw new Error("Duplicate race day list identity");
  }
  return races.sort(compareRows);
};
