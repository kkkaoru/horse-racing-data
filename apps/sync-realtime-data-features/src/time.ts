// Run with bun.
// JST = UTC+9. Helpers for date / polling-window checks.

const MS_PER_MINUTE = 60_000;
const MS_PER_DAY = 86_400_000;
const JST_OFFSET_MINUTES = 9 * 60;
const JST_POLLING_START_HOUR = 6;
const JST_POLLING_END_HOUR = 21;
const YYYYMMDD_YEAR_START = 0;
const YYYYMMDD_YEAR_END = 4;
const YYYYMMDD_MONTH_END = 6;
const YYYYMMDD_DAY_END = 8;
const ISO_DATE_MONTH_PAD = 2;

const toJstParts = (now: Date): { hour: number; ymd: string } => {
  const jst = new Date(now.getTime() + JST_OFFSET_MINUTES * MS_PER_MINUTE);
  const yyyy = jst.getUTCFullYear();
  const mm = String(jst.getUTCMonth() + 1).padStart(ISO_DATE_MONTH_PAD, "0");
  const dd = String(jst.getUTCDate()).padStart(ISO_DATE_MONTH_PAD, "0");
  return { hour: jst.getUTCHours(), ymd: `${yyyy}${mm}${dd}` };
};

export const getTodayJst = (now: Date): string => toJstParts(now).ymd;

export const isJstPollingWindow = (now: Date): boolean => {
  const { hour } = toJstParts(now);
  return hour >= JST_POLLING_START_HOUR && hour < JST_POLLING_END_HOUR;
};

export const toJstIsoString = (now: Date = new Date()): string => now.toISOString();

// Parse YYYYMMDD into a UTC midnight Date (JST calendar dates flattened to UTC).
const parseYyyymmddAsUtcMidnight = (yyyymmdd: string): Date => {
  const year = Number(yyyymmdd.slice(YYYYMMDD_YEAR_START, YYYYMMDD_YEAR_END));
  const monthIndex = Number(yyyymmdd.slice(YYYYMMDD_YEAR_END, YYYYMMDD_MONTH_END)) - 1;
  const day = Number(yyyymmdd.slice(YYYYMMDD_MONTH_END, YYYYMMDD_DAY_END));
  return new Date(Date.UTC(year, monthIndex, day));
};

const formatUtcDateAsYyyymmdd = (utcDate: Date): string => {
  const yyyy = utcDate.getUTCFullYear();
  const mm = String(utcDate.getUTCMonth() + 1).padStart(ISO_DATE_MONTH_PAD, "0");
  const dd = String(utcDate.getUTCDate()).padStart(ISO_DATE_MONTH_PAD, "0");
  return `${yyyy}${mm}${dd}`;
};

export const shiftYyyymmddByDays = (yyyymmdd: string, deltaDays: number): string => {
  const base = parseYyyymmddAsUtcMidnight(yyyymmdd);
  const shifted = new Date(base.getTime() + deltaDays * MS_PER_DAY);
  return formatUtcDateAsYyyymmdd(shifted);
};

export const computeTomorrowJst = (now: Date): string => shiftYyyymmddByDays(getTodayJst(now), 1);
