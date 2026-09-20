// Runs with bun; bounded, read-only calendar projection for a single year.
export interface RaceCalendarInput {
  namespace: string;
  year: string;
}

export interface RaceCalendarDay {
  year: string;
  month: string;
  day: string;
  jraCount: number;
  narCount: number;
}

interface RaceCalendarReader {
  input: RaceCalendarInput;
  query: (sql: string) => Promise<Record<string, unknown>[]>;
}

const YEAR_PATTERN: RegExp = /^[1-9]\d{3}$/u;
const IDENTIFIER_PATTERN: RegExp = /^[A-Za-z_][A-Za-z0-9_]*$/u;
const MONTH_DAY_PATTERN: RegExp = /^\d{4}$/u;
const COUNT_PATTERN: RegExp = /^\d+$/u;
const MAX_DAYS: number = 366;

export const buildRaceCalendarReadSql = (input: RaceCalendarInput): string => {
  if (!YEAR_PATTERN.test(input.year) || !IDENTIFIER_PATTERN.test(input.namespace)) {
    throw new Error("Invalid race calendar input");
  }
  return `SELECT kaisai_nen, kaisai_tsukihi,
  SUM(jra_count) AS jra_count, SUM(nar_count) AS nar_count
FROM (
  SELECT kaisai_nen, kaisai_tsukihi, COUNT(*) AS jra_count, 0 AS nar_count
  FROM ${input.namespace}.jvd_ra WHERE kaisai_nen = '${input.year}'
  GROUP BY kaisai_nen, kaisai_tsukihi
  UNION ALL
  SELECT kaisai_nen, kaisai_tsukihi, 0 AS jra_count, COUNT(*) AS nar_count
  FROM ${input.namespace}.nvd_ra WHERE kaisai_nen = '${input.year}'
  GROUP BY kaisai_nen, kaisai_tsukihi
) race_days
GROUP BY kaisai_nen, kaisai_tsukihi
ORDER BY kaisai_nen DESC, kaisai_tsukihi DESC`;
};

const readCount = (value: unknown): number => {
  if (typeof value === "string" && COUNT_PATTERN.test(value)) return readCount(Number(value));
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new Error("Invalid race calendar count");
  }
  return value;
};

const readDay = (row: Record<string, unknown>, year: string): RaceCalendarDay => {
  const monthDay: unknown = row.kaisai_tsukihi;
  if (
    row.kaisai_nen !== year ||
    typeof monthDay !== "string" ||
    !MONTH_DAY_PATTERN.test(monthDay)
  ) {
    throw new Error("Invalid race calendar identity");
  }
  const timestamp: number = Date.UTC(
    Number(year),
    Number(monthDay.slice(0, 2)) - 1,
    Number(monthDay.slice(2)),
  );
  if (new Date(timestamp).toISOString().slice(0, 10).replaceAll("-", "") !== year + monthDay) {
    throw new Error("Invalid race calendar date");
  }
  const jraCount: number = readCount(row.jra_count);
  const narCount: number = readCount(row.nar_count);
  if (jraCount === 0 && narCount === 0) throw new Error("Invalid empty race calendar day");
  return { year, month: monthDay.slice(0, 2), day: monthDay.slice(2), jraCount, narCount };
};

const compareDays = (left: RaceCalendarDay, right: RaceCalendarDay): number =>
  (right.month + right.day).localeCompare(left.month + left.day);

export const readRaceCalendar = async (reader: RaceCalendarReader): Promise<RaceCalendarDay[]> => {
  const sql: string = buildRaceCalendarReadSql(reader.input);
  const rows: Record<string, unknown>[] = await reader.query(sql);
  if (rows.length > MAX_DAYS) throw new Error("Race calendar exceeds day limit");
  const days: RaceCalendarDay[] = rows.map((row) => readDay(row, reader.input.year));
  if (new Set(days.map((day) => day.month + day.day)).size !== days.length) {
    throw new Error("Duplicate race calendar day");
  }
  return days.sort(compareDays);
};
