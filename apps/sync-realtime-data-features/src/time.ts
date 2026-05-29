// Run with bun.
// JST = UTC+9. Helpers for date / polling-window checks.

const MS_PER_MINUTE = 60_000;
const JST_OFFSET_MINUTES = 9 * 60;
const JST_POLLING_START_HOUR = 6;
const JST_POLLING_END_HOUR = 21;

const toJstParts = (now: Date): { hour: number; ymd: string } => {
  const jst = new Date(now.getTime() + JST_OFFSET_MINUTES * MS_PER_MINUTE);
  const yyyy = jst.getUTCFullYear();
  const mm = String(jst.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(jst.getUTCDate()).padStart(2, "0");
  return { hour: jst.getUTCHours(), ymd: `${yyyy}${mm}${dd}` };
};

export const getTodayJst = (now: Date): string => toJstParts(now).ymd;

export const isJstPollingWindow = (now: Date): boolean => {
  const { hour } = toJstParts(now);
  return hour >= JST_POLLING_START_HOUR && hour < JST_POLLING_END_HOUR;
};

export const toJstIsoString = (now: Date = new Date()): string => now.toISOString();
