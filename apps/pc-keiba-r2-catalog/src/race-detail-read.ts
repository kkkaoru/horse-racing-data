// Runs with bun; read-only Catalog projection preserving absence versus provider failure.
import { dedupeIdenticalRows } from "./normalise";

export interface RaceDetailReadInput {
  namespace: string;
  source: "jra" | "nar";
  date: string;
  keibajoCode: string;
  raceBango: string;
}
export interface RaceDetailReadOptions {
  input: RaceDetailReadInput;
  query: (sql: string) => Promise<unknown[]>;
}
interface DetailColumn {
  column: string;
  property: string;
}
const COLUMNS: readonly DetailColumn[] = [
  { column: "kaisai_nen", property: "kaisaiNen" },
  { column: "kaisai_tsukihi", property: "kaisaiTsukihi" },
  { column: "keibajo_code", property: "keibajoCode" },
  { column: "kaisai_kai", property: "kaisaiKai" },
  { column: "kaisai_nichime", property: "kaisaiNichime" },
  { column: "race_bango", property: "raceBango" },
  { column: "kyosomei_hondai", property: "kyosomeiHondai" },
  { column: "kyosomei_fukudai", property: "kyosomeiFukudai" },
  { column: "kyosomei_kakkonai", property: "kyosomeiKakkonai" },
  { column: "grade_code", property: "gradeCode" },
  { column: "kyoso_shubetsu_code", property: "kyosoShubetsuCode" },
  { column: "kyoso_kigo_code", property: "kyosoKigoCode" },
  { column: "juryo_shubetsu_code", property: "juryoShubetsuCode" },
  { column: "kyoso_joken_code", property: "kyosoJokenCode" },
  { column: "kyoso_joken_meisho", property: "kyosoJokenMeisho" },
  { column: "kyori", property: "kyori" },
  { column: "track_code", property: "trackCode" },
  { column: "hasso_jikoku", property: "hassoJikoku" },
  { column: "toroku_tosu", property: "torokuTosu" },
  { column: "shusso_tosu", property: "shussoTosu" },
  { column: "tenko_code", property: "tenkoCode" },
  { column: "babajotai_code_shiba", property: "babajotaiCodeShiba" },
  { column: "babajotai_code_dirt", property: "babajotaiCodeDirt" },
];
const IDENTIFIER: RegExp = /^[A-Za-z_][A-Za-z0-9_]*$/u;
const DATE: RegExp = /^\d{8}$/u;
const VENUE_CODE: RegExp = /^[0-9A-Z]{2}$/u;
const RACE_NUMBER: RegExp = /^\d{2}$/u;
const DUPLICATE_DETECTION_LIMIT: number = 2;

const validDate = (date: string): boolean => {
  if (!DATE.test(date)) return false;
  const iso: string = `${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}`;
  const parsed: Date = new Date(`${iso}T00:00:00.000Z`);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === iso;
};

export const buildRaceDetailReadSql = (input: RaceDetailReadInput): string => {
  if (
    !IDENTIFIER.test(input.namespace) ||
    !validDate(input.date) ||
    !VENUE_CODE.test(input.keibajoCode) ||
    !RACE_NUMBER.test(input.raceBango) ||
    (input.source !== "jra" && input.source !== "nar")
  )
    throw new Error("Invalid race detail input");
  const table: string = input.source === "jra" ? "jvd_ra" : "nvd_ra";
  return `SELECT ${COLUMNS.map(({ column }) => column).join(", ")}\nFROM ${input.namespace}.${table}\nWHERE kaisai_nen = '${input.date.slice(0, 4)}'\n  AND kaisai_tsukihi = '${input.date.slice(4)}'\n  AND keibajo_code = '${input.keibajoCode}'\n  AND race_bango = '${input.raceBango}'\nLIMIT ${DUPLICATE_DETECTION_LIMIT}`;
};

const parseDetailRow = (value: unknown): Record<string, string | null> => {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new Error("Invalid race detail row");
  return Object.fromEntries(
    COLUMNS.map(({ column, property }) => {
      const field: unknown = Reflect.get(value, column);
      if (field !== null && typeof field !== "string")
        throw new Error("Missing or invalid race detail field");
      return [property, field];
    }),
  );
};

export const readRaceDetail = async (
  options: RaceDetailReadOptions,
): Promise<Record<string, string | null> | null> => {
  const rows: unknown[] = await options.query(buildRaceDetailReadSql(options.input));
  if (rows.length === 0) return null;
  // R2 SQL's distributed planner can return the same physical row more than once, so only a
  // genuine identity conflict (two different rows for one race) is ambiguous.
  const [row, ...conflicts] = dedupeIdenticalRows(rows.map(parseDetailRow));
  if (row === undefined || conflicts.length > 0) throw new Error("Ambiguous race detail identity");
  if (
    row.kaisaiNen !== options.input.date.slice(0, 4) ||
    row.kaisaiTsukihi !== options.input.date.slice(4) ||
    row.keibajoCode !== options.input.keibajoCode ||
    row.raceBango !== options.input.raceBango
  )
    throw new Error("Race detail identity mismatch");
  return { ...row, source: options.input.source };
};
