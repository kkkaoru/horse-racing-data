// Runs with bun; read-only year summaries preserve the legacy cross-source day union.
export interface RaceYearSummary {
  year: string;
  raceCount: number;
  dayCount: number;
}

interface RaceYearsReader {
  namespace: string;
  query: (sql: string) => Promise<Record<string, unknown>[]>;
}

const IDENTIFIER_PATTERN: RegExp = /^[A-Za-z_][A-Za-z0-9_]*$/u;
const YEAR_PATTERN: RegExp = /^[1-9]\d{3}$/u;
const COUNT_PATTERN: RegExp = /^\d+$/u;
const MAX_YEARS: number = 256;

export const buildRaceYearsReadSql = (namespace: string): string => {
  if (!IDENTIFIER_PATTERN.test(namespace)) throw new Error("Invalid race years namespace");
  return `SELECT kaisai_nen AS year, SUM(race_count) AS race_count,
  COUNT(DISTINCT kaisai_tsukihi) AS day_count
FROM (
  SELECT kaisai_nen, kaisai_tsukihi, COUNT(*) AS race_count
  FROM ${namespace}.jvd_ra GROUP BY kaisai_nen, kaisai_tsukihi
  UNION ALL
  SELECT kaisai_nen, kaisai_tsukihi, COUNT(*) AS race_count
  FROM ${namespace}.nvd_ra GROUP BY kaisai_nen, kaisai_tsukihi
) race_days
GROUP BY kaisai_nen
ORDER BY kaisai_nen DESC`;
};

const readCount = (value: unknown): number => {
  if (typeof value === "string" && COUNT_PATTERN.test(value)) return readCount(Number(value));
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0) {
    throw new Error("Invalid race years count");
  }
  return value;
};

const readYear = (row: Record<string, unknown>): RaceYearSummary => {
  if (typeof row.year !== "string" || !YEAR_PATTERN.test(row.year)) {
    throw new Error("Invalid race years identity");
  }
  const year: number = Number(row.year);
  const leap: boolean = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const dayCount: number = readCount(row.day_count);
  const raceCount: number = readCount(row.race_count);
  if (dayCount > (leap ? 366 : 365) || raceCount < dayCount) {
    throw new Error("Inconsistent race years counts");
  }
  return { year: row.year, raceCount, dayCount };
};

const compareYears = (left: RaceYearSummary, right: RaceYearSummary): number =>
  right.year.localeCompare(left.year);

export const readRaceYears = async (reader: RaceYearsReader): Promise<RaceYearSummary[]> => {
  const rows: Record<string, unknown>[] = await reader.query(
    buildRaceYearsReadSql(reader.namespace),
  );
  if (rows.length > MAX_YEARS) throw new Error("Race years exceeds result limit");
  const years: RaceYearSummary[] = rows.map(readYear);
  if (new Set(years.map((row) => row.year)).size !== years.length) {
    throw new Error("Duplicate race year");
  }
  return years.sort(compareYears);
};
