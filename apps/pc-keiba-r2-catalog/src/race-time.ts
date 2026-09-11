// This file runs with bun.

const ENCODED_RACE_TIME_PATTERN: RegExp = /^\d{1,4}$/u;
const TENTHS_PER_SECOND: number = 10;
const SECONDS_PER_MINUTE: number = 60;
const TENTHS_PER_MINUTE: number = SECONDS_PER_MINUTE * TENTHS_PER_SECOND;
const PADDED_RACE_TIME_LENGTH: number = 4;
const SECONDS_START_INDEX: number = 1;
const TENTHS_INDEX: number = 3;

const encodedRaceTimeText = (value: unknown): string => {
  if (typeof value === "string") return value.trim();
  if (typeof value === "number" || typeof value === "bigint") return String(value);
  return "";
};

export const parseEncodedRaceTimeTenths = (value: unknown): number | null => {
  const cleaned = encodedRaceTimeText(value);
  if (!ENCODED_RACE_TIME_PATTERN.test(cleaned) || /^0+$/u.test(cleaned)) {
    return null;
  }
  const padded = cleaned.padStart(PADDED_RACE_TIME_LENGTH, "0");
  const minutes = Number(padded.slice(0, SECONDS_START_INDEX));
  const seconds = Number(padded.slice(SECONDS_START_INDEX, TENTHS_INDEX));
  const tenths = Number(padded.slice(TENTHS_INDEX));
  return seconds < SECONDS_PER_MINUTE
    ? minutes * TENTHS_PER_MINUTE + seconds * TENTHS_PER_SECOND + tenths
    : null;
};

export const parseEncodedRaceTimeSeconds = (value: unknown): number | null => {
  const tenths = parseEncodedRaceTimeTenths(value);
  return tenths === null ? null : tenths / TENTHS_PER_SECOND;
};

export const encodedRaceTimeTenthsSql = (numericExpression: string): string => `(CASE
    WHEN ${numericExpression} IS NULL
      OR ${numericExpression} <= 0
      OR floor(${numericExpression} / 10) % 100 >= 60
    THEN NULL
    ELSE floor(${numericExpression} / 1000) * 600
      + floor(${numericExpression} / 10) % 100 * 10
      + ${numericExpression} % 10
  END)`;
