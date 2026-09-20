// Run with bun. R2 SQL expression helpers for the time-score readers.
// R2 SQL has no `regexp_replace` and no `~`, so the viewer's PostgreSQL
// expressions are re-expressed with try_cast/substring and, where date math is
// needed, computed in TypeScript instead.

const MONTHS_IN_WINDOW: number = 1;

// PostgreSQL `raceTimeTenthsSql`: the raw soha_time is a tenths-of-a-second
// clock value whose fourth digit is the fraction, so "1234" is 83.4s. The SQL
// mirrors the viewer's guards: 1-4 digits, not all zeros, and minutes < 60.
export const raceTimeTenthsSql = (column: string): string => {
  const cleaned: string = `btrim(coalesce(${column}, ''))`;
  const padded: string = `lpad(${cleaned}, 4, '0')`;
  return `CASE
    WHEN length(${cleaned}) BETWEEN 1 AND 4
      AND replace(${cleaned}, '0', '') <> ''
      AND try_cast(substring(${padded} FROM 2 FOR 2) AS INT) < 60
    THEN try_cast(substring(${padded} FROM 1 FOR 1) AS INT) * 600
      + try_cast(substring(${padded} FROM 2 FOR 2) AS INT) * 10
      + try_cast(substring(${padded} FROM 4 FOR 1) AS INT)
    ELSE NULL
  END`;
};

// PostgreSQL `nullif(regexp_replace(coalesce(x, ''), '[^0-9]', '', 'g'), '')::numeric`
// for values that the mirror stores as plain digit strings.
export const digitsOnlyNumericSql = (column: string): string =>
  `try_cast(nullif(btrim(coalesce(${column}, '')), '') AS DOUBLE)`;

// The viewer builds this window with `to_char(to_date(raceDate) ± interval
// '1 month', 'MM')`; the date maths is done here instead.
export const monthWindowMonths = (raceDate: string): string[] => {
  const match: RegExpMatchArray | null = /^(\d{4})(\d{2})(\d{2})$/u.exec(raceDate);
  if (match === null) throw new Error("Invalid race history date");
  const year: number = Number(match[1]);
  const month: number = Number(match[2]);
  const day: number = Number(match[3]);
  const parsed: Date = new Date(Date.UTC(year, month - 1, day));
  if (
    !Number.isFinite(parsed.getTime()) ||
    parsed.getUTCFullYear() !== year ||
    parsed.getUTCMonth() !== month - 1 ||
    parsed.getUTCDate() !== day
  )
    throw new Error("Invalid race history date");
  const months: string[] = [];
  for (let offset = -MONTHS_IN_WINDOW; offset <= MONTHS_IN_WINDOW; offset += 1) {
    const shifted: Date = new Date(Date.UTC(year, month - 1 + offset, 1));
    months.push(String(shifted.getUTCMonth() + 1).padStart(2, "0"));
  }
  return [...new Set(months)];
};

export const monthWindowConditionSql = (
  column: string,
  raceDate: string,
  enabled: boolean,
): string | null => {
  if (!enabled) return null;
  const months: string[] = monthWindowMonths(raceDate);
  return `substring(${column} FROM 1 FOR 2) IN (${months.map((month) => `'${month}'`).join(", ")})`;
};
