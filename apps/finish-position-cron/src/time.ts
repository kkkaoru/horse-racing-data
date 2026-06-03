// Run with bun. JST run-date helpers for the cron schedule.

const JST_OFFSET_MS = 9 * 60 * 60 * 1000;
const ISO_DATE_LENGTH = 10;
const DATE_SEPARATOR = "-";

// Shift a UTC instant into JST wall-clock by adding the +09:00 offset, so the
// resulting ISO date string reflects the Japanese calendar day the cron fires
// on (the "0 18 * * *" UTC trigger maps to JST 03:00 the next day).
const toJstDate = (scheduledAt: Date): Date => new Date(scheduledAt.getTime() + JST_OFFSET_MS);

// "YYYY-MM-DD" in JST — used for the audit run_date column.
export const getRunDateJst = (scheduledAt: Date): string =>
  toJstDate(scheduledAt).toISOString().slice(0, ISO_DATE_LENGTH);

// "YYYYMMDD" in JST — used as the container RUN_DATE env var (8-digit form the
// feature pipeline expects).
export const getRunYmdJst = (scheduledAt: Date): string =>
  getRunDateJst(scheduledAt).split(DATE_SEPARATOR).join("");
