// Runs with bun. Ordered names match the verified PostgreSQL C.UTF-8 collation.
import { Buffer } from "node:buffer";
import {
  validateRaceDayListInput,
  readRaceDayList,
  buildRaceDayListReadSql,
  type RaceDayListRow,
  type RaceDayListInput,
  type RaceDayListReader,
} from "./race-day-list-read";

export interface RaceDayJockeyRow {
  source: "jra" | "nar";
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  jockeyNames: string[];
}
const MAX_GROUPS: number = 4096;
const VENUE: RegExp = /^[0-9A-Z]{2}$/u;
const RACE: RegExp = /^\d{2}$/u;
const KEYS: string = "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango";
const EDGE_SPACES: RegExp = /^ +| +$/gu;

const buildJockeyAggregateSql = (input: RaceDayListInput): string => {
  validateRaceDayListInput(input);
  const predicate: string = `kaisai_nen = '${input.date.slice(0, 4)}' AND kaisai_tsukihi = '${input.date.slice(4)}'`;
  return `WITH entries AS (
SELECT 'jra' AS source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, kishumei_ryakusho FROM ${input.namespace}.jvd_se WHERE ${predicate}
UNION ALL
SELECT 'nar' AS source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, kishumei_ryakusho FROM ${input.namespace}.nvd_se WHERE ${predicate}
), cleaned AS (SELECT ${KEYS}, btrim(kishumei_ryakusho, ' ') AS jockey_name FROM entries)
SELECT ${KEYS}, array_agg(DISTINCT jockey_name) AS names FROM cleaned
WHERE jockey_name IS NOT NULL AND jockey_name <> '' GROUP BY ${KEYS}`;
};

export const buildRaceDayJockeysReadSql = (input: RaceDayListInput): string =>
  `${buildJockeyAggregateSql(input)} LIMIT ${MAX_GROUPS + 1}`;

export const buildRaceDayListWithJockeysReadSql = (input: RaceDayListInput): string =>
  `WITH races AS (${buildRaceDayListReadSql(input)}), jockeys AS (${buildJockeyAggregateSql(input)})
SELECT races.*, jockeys.names FROM races LEFT JOIN jockeys
ON races.source=jockeys.source AND races.kaisai_nen=jockeys.kaisai_nen AND races.kaisai_tsukihi=jockeys.kaisai_tsukihi AND races.keibajo_code=jockeys.keibajo_code AND races.race_bango=jockeys.race_bango
ORDER BY races.hasso_jikoku ASC NULLS LAST,races.keibajo_code ASC,races.race_bango ASC,races.source ASC LIMIT ${MAX_GROUPS + 1}`;

const compareUtf8 = (left: string, right: string): number =>
  Buffer.compare(Buffer.from(left, "utf8"), Buffer.from(right, "utf8"));
const isName = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0 && value.replace(EDGE_SPACES, "") === value;
const parseRow = (value: unknown, date: string): RaceDayJockeyRow => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("Invalid day jockey row");
  }
  const source: unknown = Reflect.get(value, "source");
  const year: unknown = Reflect.get(value, "kaisai_nen");
  const monthDay: unknown = Reflect.get(value, "kaisai_tsukihi");
  const venue: unknown = Reflect.get(value, "keibajo_code");
  const race: unknown = Reflect.get(value, "race_bango");
  const names: unknown = Reflect.get(value, "names");
  if (
    (source !== "jra" && source !== "nar") ||
    year !== date.slice(0, 4) ||
    monthDay !== date.slice(4) ||
    typeof venue !== "string" ||
    !VENUE.test(venue) ||
    typeof race !== "string" ||
    !RACE.test(race)
  )
    throw new Error("Invalid day jockey identity");
  if (
    !Array.isArray(names) ||
    names.length === 0 ||
    !names.every(isName) ||
    new Set(names).size !== names.length
  ) {
    throw new Error("Invalid day jockey names");
  }
  return {
    source,
    kaisaiNen: year,
    kaisaiTsukihi: monthDay,
    keibajoCode: venue,
    raceBango: race,
    jockeyNames: names.toSorted(compareUtf8),
  };
};
const identity = (row: RaceDayJockeyRow): string =>
  `${row.source}/${row.keibajoCode}/${row.raceBango}`;
const compareRows = (left: RaceDayJockeyRow, right: RaceDayJockeyRow): number =>
  compareUtf8(identity(left), identity(right));

export const readRaceDayJockeys = async (
  reader: RaceDayListReader,
): Promise<RaceDayJockeyRow[]> => {
  const rows: unknown[] = await reader.query(buildRaceDayJockeysReadSql(reader.input));
  if (rows.length > MAX_GROUPS) throw new Error("Day jockey groups exceed row limit");
  const groups: RaceDayJockeyRow[] = rows.map((row) => parseRow(row, reader.input.date));
  if (new Set(groups.map(identity)).size !== groups.length)
    throw new Error("Duplicate day jockey identity");
  return groups.sort(compareRows);
};

// Only race rows drive the result. Orphan entry groups are not extra races.
// One remote statement avoids between-query skew, not cross-table publication skew.
// In-memory adapters reuse the independently validated projections of that result.
export const readRaceDayListWithJockeys = async (
  reader: RaceDayListReader,
): Promise<RaceDayListRow[]> => {
  const joined: unknown[] = await reader.query(buildRaceDayListWithJockeysReadSql(reader.input));
  const [races, groups] = await Promise.all([
    readRaceDayList({ input: reader.input, query: async () => joined }),
    readRaceDayJockeys({
      input: reader.input,
      query: async () =>
        joined.filter(
          (value) =>
            typeof value !== "object" || value === null || Reflect.get(value, "names") !== null,
        ),
    }),
  ]);
  const names: Map<string, string[]> = new Map(
    groups.map((group) => [identity(group), group.jockeyNames]),
  );
  return races.map((race) => ({ ...race, jockeyNames: names.get(identity(race)) ?? [] }));
};
