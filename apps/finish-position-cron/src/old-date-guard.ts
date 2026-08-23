// Run with bun. Guards the predict queue consumer against dispatching stale
// historical runYmd messages (retries, DLQ redrives, stale manual triggers)
// to the Container. Past/historical races do not need CF prediction
// generation at all -- that is Mac-local backfill territory -- so a
// zombie message with a runYmd far in the past would otherwise waste a
// full 10-27 minute Container pipeline run on a race that has long since
// settled.

import { getRunYmdJst } from "./time";

const RUN_YMD_PATTERN = /^\d{8}$/u;
const RUN_YMD_YEAR_END = 4;
const RUN_YMD_MONTH_END = 6;
const RUN_YMD_LENGTH = 8;
const MONTH_INDEX_OFFSET = 1;
const MS_PER_DAY = 24 * 60 * 60 * 1000;

// Normal Queue traffic is useful only for today's or a future card. Allowing
// yesterday's failed deliveries to retain their retry budget can starve the
// current card after midnight. Explicit operator repairs remain available via
// the message `force` flag, which bypasses this guard in queue-consumer.ts.
export const OLD_DATE_THRESHOLD_DAYS = 0;

interface YmdParts {
  day: number;
  month: number;
  year: number;
}

const parseYmdParts = (runYmd: string): YmdParts => ({
  day: Number(runYmd.slice(RUN_YMD_MONTH_END, RUN_YMD_LENGTH)),
  month: Number(runYmd.slice(RUN_YMD_YEAR_END, RUN_YMD_MONTH_END)),
  year: Number(runYmd.slice(0, RUN_YMD_YEAR_END)),
});

// Anchors a Y-M-D at UTC midnight. This is NOT the real UTC instant of JST
// midnight (that would be UTC midnight minus 9h) -- it is a self-consistent
// day-counter anchor: as long as every caller derives its anchor via the
// same Y-M-D -> Date.UTC path (see isOldDateRunYmd below, which converts
// `now` through getRunYmdJst first, mirroring the JST-offset arithmetic in
// time.ts), millisecond differences between two anchors equal exact
// whole-day differences between their JST calendar days, with no DST or
// timezone edge cases (Japan observes no DST).
const buildUtcMidnightAnchor = (parts: YmdParts): Date =>
  new Date(Date.UTC(parts.year, parts.month - MONTH_INDEX_OFFSET, parts.day));

const isRealCalendarDate = (parts: YmdParts, anchor: Date): boolean =>
  anchor.getUTCFullYear() === parts.year &&
  anchor.getUTCMonth() === parts.month - MONTH_INDEX_OFFSET &&
  anchor.getUTCDate() === parts.day;

// Parses an 8-digit YYYYMMDD string (a JST calendar day, e.g. the runYmd
// produced by getRunYmdJst) into its UTC-midnight anchor Date. Returns null
// for anything that is not exactly 8 digits or does not round-trip to a
// real calendar date (e.g. "20260231" rolls over to March and is
// rejected). A malformed runYmd is treated as NOT old rather than silently
// misclassified: it is a distinct bug class this guard should surface by
// letting the message through un-skipped, not paper over by eating it.
export const parseRunYmdToUtcDate = (runYmd: string): Date | null => {
  if (!RUN_YMD_PATTERN.test(runYmd)) return null;
  const parts = parseYmdParts(runYmd);
  const anchor = buildUtcMidnightAnchor(parts);
  return isRealCalendarDate(parts, anchor) ? anchor : null;
};

// True when runYmd's JST calendar day is earlier than today's JST calendar
// day. Future-dated runYmd is never old. Explicit `force` messages bypass this
// guard at the consumer. `now` is required (not defaulted) so callers inject
// `new Date()` explicitly and tests can inject a fixed date
// deterministically.
export const isOldDateRunYmd = (runYmd: string, now: Date): boolean => {
  const runYmdAnchor = parseRunYmdToUtcDate(runYmd);
  if (runYmdAnchor === null) return false;
  const nowAnchor = buildUtcMidnightAnchor(parseYmdParts(getRunYmdJst(now)));
  const thresholdMs = nowAnchor.getTime() - OLD_DATE_THRESHOLD_DAYS * MS_PER_DAY;
  return runYmdAnchor.getTime() < thresholdMs;
};
