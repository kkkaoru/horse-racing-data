// JST date formatters used by the scheduled worker handler. Kept separate
// from src/worker.ts so the formatting logic can be unit-tested without
// pulling in the OpenNext-generated worker bundle.
const MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000;

const JST_DATE_FORMATTER = new Intl.DateTimeFormat("en-CA", {
  day: "2-digit",
  month: "2-digit",
  timeZone: "Asia/Tokyo",
  year: "numeric",
});

export const formatJstDate = (date: Date): string => JST_DATE_FORMATTER.format(date);

export const formatTodayJstDate = (now: Date): string => formatJstDate(now);

export const formatTomorrowJstDate = (now: Date): string =>
  formatJstDate(new Date(now.getTime() + MILLISECONDS_PER_DAY));
