// Run with bun. Pure conversions and weights for the Catalog time-score
// reader; these reproduce the SQL in `getTimeScoreRows` so the same numbers
// can be computed from the raw mirror strings without a Neon round trip.
const EDGE_SPACES: RegExp = /^[ 　]+|[ 　]+$/gu;
const LEADING_ZEROS: RegExp = /^0+/u;
const DIGITS: RegExp = /[0-9]/u;
const DIGITS_ONLY: RegExp = /^[0-9]+$/u;
const COMPACT_DATE: RegExp = /^(\d{4})(\d{2})(\d{2})$/u;
const MILLISECONDS_PER_DAY: number = 24 * 60 * 60 * 1000;
const DEFAULT_AGE_BAND_DAYS: number = 365;
const YOUNG_AGE_BAND_DAYS: number = 180;
const FOUR_YEAR_OLD_BAND_DAYS: number = 270;
const YOUNG_AGE_LIMIT: number = 3;
const FOUR_YEAR_OLD: number = 4;
const MIN_DISTANCE_DENOMINATOR: number = 400;
const DISTANCE_TOLERANCE_RATIO: number = 0.5;
const UNKNOWN_DISTANCE_SCORE: number = 0.5;

// PostgreSQL `coalesce(nullif(regexp_replace(umaban, '^0+', ''), ''), '0')`.
export const normaliseHorseNumber = (umaban: string | null): string => {
  const stripped: string = (umaban ?? "").replace(LEADING_ZEROS, "");
  return stripped === "" ? "0" : stripped;
};

// PostgreSQL `umaban::int`, but a malformed mirror value becomes null instead
// of aborting the whole computation.
export const parseHorseNumberSort = (umaban: string | null): number | null => {
  const trimmed: string = (umaban ?? "").trim();
  if (!DIGITS_ONLY.test(trimmed)) return null;
  return Number(trimmed);
};

// PostgreSQL `coalesce(nullif(regexp_replace(bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-')`.
export const normaliseHorseName = (bamei: string | null): string => {
  const trimmed: string = (bamei ?? "").replace(EDGE_SPACES, "");
  return trimmed === "" ? "-" : trimmed;
};

// PostgreSQL `raceTimeTenthsSql`: the raw soha_time is a tenths-of-a-second
// clock value whose fourth digit is the fraction, so "1234" is 83.4s.
export const parseRaceTimeTenths = (sohaTime: string | null): number | null => {
  const cleaned: string = (sohaTime ?? "").trim();
  if (cleaned === "" || cleaned.length > 4 || !DIGITS_ONLY.test(cleaned)) return null;
  if (!DIGITS.test(cleaned)) return null;
  if (cleaned.replaceAll("0", "") === "") return null;
  const padded: string = cleaned.padStart(4, "0");
  const minutes: number = Number(padded.slice(0, 1));
  const seconds: number = Number(padded.slice(1, 3));
  const tenths: number = Number(padded.slice(3, 4));
  if (seconds >= 60) return null;
  return minutes * 600 + seconds * 10 + tenths;
};

// PostgreSQL `nullif(regexp_replace(coalesce(x, ''), '[^0-9]', '', 'g'), '')::numeric`.
export const parseDigitsOnly = (value: string | null): number | null => {
  const digits: string = (value ?? "").replace(/[^0-9]/gu, "");
  return digits === "" ? null : Number(digits);
};

// Recency divisor band for the current horse's age.
export const ageBandDays = (age: number | null): number => {
  if (age === null) return DEFAULT_AGE_BAND_DAYS;
  if (age <= YOUNG_AGE_LIMIT) return YOUNG_AGE_BAND_DAYS;
  if (age === FOUR_YEAR_OLD) return FOUR_YEAR_OLD_BAND_DAYS;
  return DEFAULT_AGE_BAND_DAYS;
};

export const compactDateToEpochMs = (date: string | null): number | null => {
  const match: RegExpMatchArray | null = COMPACT_DATE.exec(date ?? "");
  if (match === null) return null;
  const year: number = Number(match[1]);
  const month: number = Number(match[2]);
  const day: number = Number(match[3]);
  const epochMs: number = Date.UTC(year, month - 1, day);
  const parsed: Date = new Date(epochMs);
  if (
    parsed.getUTCFullYear() !== year ||
    parsed.getUTCMonth() !== month - 1 ||
    parsed.getUTCDate() !== day
  )
    return null;
  return epochMs;
};

// Whole days between two YYYYMMDD values (PostgreSQL `to_date(a) - to_date(b)`).
export const dayGap = (raceDate: string | null, pastRaceDate: string | null): number | null => {
  const left: number | null = compactDateToEpochMs(raceDate);
  const right: number | null = compactDateToEpochMs(pastRaceDate);
  if (left === null || right === null) return null;
  return Math.round((left - right) / MILLISECONDS_PER_DAY);
};

// PostgreSQL `1.0 / (1.0 + greatest(0, dayGap) / band)`.
export const recencyWeight = (gapDays: number | null, bandDays: number): number =>
  1 / (1 + Math.max(0, gapDays ?? 0) / bandDays);

// PostgreSQL `greatest(0, 1 - |past - target| / greatest(target * 0.5, 400))`,
// with the 0.5 fallback when either distance is unknown.
export const distanceScore = (
  pastDistance: number | null,
  targetDistance: number | null,
): number => {
  if (pastDistance === null || targetDistance === null) return UNKNOWN_DISTANCE_SCORE;
  const denominator: number = Math.max(
    targetDistance * DISTANCE_TOLERANCE_RATIO,
    MIN_DISTANCE_DENOMINATOR,
  );
  return Math.max(0, 1 - Math.abs(pastDistance - targetDistance) / denominator);
};
