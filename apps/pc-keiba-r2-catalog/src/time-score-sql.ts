// Run with bun. R2 SQL expression helpers for the time-score readers.
// R2 SQL has no `regexp_replace` and no `~`, so the viewer's PostgreSQL
// expressions are re-expressed with try_cast/substring and, where date math is
// needed, computed in TypeScript instead.

interface WeightNumericSqlInput {
  column: string;
  venueColumn: string;
  carried: boolean;
}

const MONTHS_IN_WINDOW: number = 1;
const HEX_DIGITS: string = "0123456789ABCDEF";
const HEX_TRIPLET_WIDTH: number = 3;
const HEX_PLACE_VALUES: number[] = [256, 16, 1];

const onlyCharactersSql = (expression: string, characters: string): string =>
  `${[...characters].reduce((sql, character) => `replace(${sql}, '${character}', '')`, expression)} = ''`;

// Decodes a three-character upper-case JV-Data hex field ('3B4' = 948).
// R2 SQL's try_cast never parses a '0x' literal (it always yields NULL), so
// each digit is looked up by position instead.
export const hexTripletSql = (cleaned: string): string =>
  `CASE WHEN length(${cleaned}) = ${String(HEX_TRIPLET_WIDTH)} AND ${onlyCharactersSql(cleaned, HEX_DIGITS)}
    THEN ${HEX_PLACE_VALUES.map(
      (placeValue, index) =>
        `(strpos('${HEX_DIGITS}', substr(${cleaned}, ${String(index + 1)}, 1)) - 1) * ${String(placeValue)}`,
    ).join(" + ")}
    ELSE NULL END`;

// PostgreSQL `raceTimeTenthsSql`: the raw soha_time is a tenths-of-a-second
// clock value whose fourth digit is the fraction, so "1234" is 83.4s. The SQL
// mirrors the viewer's guards: 1-4 digits, not all zeros, and seconds < 60.
export const raceTimeTenthsSql = (column: string): string => {
  const cleaned: string = `btrim(coalesce(${column}, ''))`;
  const padded: string = `lpad(${cleaned}, 4, '0')`;
  return `CASE
    WHEN length(${cleaned}) BETWEEN 1 AND 4
      AND ${onlyCharactersSql(cleaned, "0123456789")}
      AND replace(${cleaned}, '0', '') <> ''
      AND try_cast(substring(${padded} FROM 2 FOR 2) AS INT) < 60
    THEN try_cast(substring(${padded} FROM 1 FOR 1) AS INT) * 600
      + try_cast(substring(${padded} FROM 2 FOR 2) AS INT) * 10
      + try_cast(substring(${padded} FROM 4 FOR 1) AS INT)
    ELSE NULL
  END`;
};

// Validate the entire raw field; never accept exponents or strip corruption.
export const digitsOnlyNumericSql = (column: string): string => {
  const cleaned: string = `btrim(coalesce(${column}, ''))`;
  return `CASE WHEN ${onlyCharactersSql(cleaned, "0123456789")}
    THEN try_cast(nullif(${cleaned}, '') AS DOUBLE) ELSE NULL END`;
};

export const positiveDecimalSql = (column: string): string => {
  const value: string = digitsOnlyNumericSql(column);
  return `CASE WHEN (${value}) > 0 THEN (${value}) ELSE NULL END`;
};

export const weightNumericSql = (input: WeightNumericSqlInput): string => {
  const cleaned: string = `upper(btrim(coalesce(${input.column}, '')))`;
  const hex: string = hexTripletSql(cleaned);
  return `CASE WHEN btrim(${input.venueColumn}) = '83' THEN
    CASE WHEN ${cleaned} NOT IN ('', '000', 'FFF')
      AND (${hex}) > 0
      THEN (${hex}) * ${input.carried ? 10 : 1} ELSE NULL END
    ELSE ${positiveDecimalSql(input.column)} END`;
};

export const marginNumericSql = (column: string): string => {
  const cleaned: string = `btrim(coalesce(${column}, ''))`;
  const magnitude: string = `CASE WHEN substring(${cleaned} FROM 1 FOR 1) IN ('+', '-')
    THEN substring(${cleaned} FROM 2) ELSE ${cleaned} END`;
  return `CASE WHEN ${onlyCharactersSql(`(${magnitude})`, "0123456789")}
    THEN ${digitsOnlyNumericSql(`(${magnitude})`)} ELSE NULL END`;
};

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
