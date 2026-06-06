// Run with: bun run --filter sync-realtime-data-hot test
// Polling window gate. Aligns the every-minute odds cron with the actual
// betting-sale windows so off-hours stay cold and deepen R2 suspend.
//
//   - NAR: same-day sale opens at JST 10:00, races end by ~22:00.
//   - JRA: weekend / monday-holiday only, advance sale opens the previous
//     evening at JST 19:00 and runs through race day.
//
// Gate is true when ANY of the following holds (logical OR):
//
//   (A) JRA race is scheduled today (JST) AND now is in
//       [09:00 JST, last_jra_today_start + 30min].
//   (B) JRA race is scheduled tomorrow (JST) AND now is in
//       [19:00 JST, 23:59:59 JST] of today. This is the prep window for
//       advance betting that opens the evening before.
//   (C) NAR race is scheduled today (JST) AND now is in
//       [10:00 JST, last_nar_today_start + 30min].
//
// All other times the gate returns false so the cron tick is a no-op.
//
// The D1 lookup is cached in KV for 60 s under a single key. That keeps the
// gate at one D1 read per minute even when multiple cron ticks land in the
// same second, and the 60 s freshness lag is acceptable because the window
// boundaries are hour-aligned and the populate cron writes the next day's
// rows during the previous evening (well before the 19:00 prep window opens).
import type { Env } from "../types";
import { getJstDateParts } from "../time";

interface PollingWindowRow {
  source: string;
  yyyy_mm_dd: string;
  last_start: string;
}

interface SameDayWindowInput {
  hourLowerBound: number;
  hourNow: number;
  lastStartIso: string;
  now: Date;
}

const POLLING_WINDOW_KV_KEY = "odds-polling-window:active";
const POLLING_WINDOW_KV_TTL_SECONDS = 60;
const KV_VALUE_TRUE = "true";
const KV_VALUE_FALSE = "false";
const JRA_BETTING_OPEN_PREP_HOUR = 19;
const JRA_BETTING_OPEN_TODAY_HOUR = 9;
const NAR_BETTING_OPEN_TODAY_HOUR = 10;
const POST_RACE_GRACE_MINUTES = 30;
const MINUTE_MS = 60_000;
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const JRA_SOURCE = "jra";
const NAR_SOURCE = "nar";
const QUERY_RACE_WINDOW = `
  SELECT
    source,
    substr(race_start_at_jst, 1, 10) AS yyyy_mm_dd,
    MAX(race_start_at_jst) AS last_start
  FROM odds_fetch_state
  WHERE substr(race_start_at_jst, 1, 10) IN (?, ?)
  GROUP BY source, yyyy_mm_dd
`;

const toYyyyMmDd = (parts: ReturnType<typeof getJstDateParts>): string =>
  `${parts.year}-${parts.month}-${parts.day}`;

const tomorrowParts = (now: Date): ReturnType<typeof getJstDateParts> =>
  getJstDateParts(new Date(now.getTime() + MS_PER_DAY));

const findRow = (
  rows: PollingWindowRow[],
  source: string,
  yyyyMmDd: string,
): PollingWindowRow | undefined =>
  rows.find((row) => row.source === source && row.yyyy_mm_dd === yyyyMmDd);

const isWithinSameDayWindow = ({
  hourLowerBound,
  hourNow,
  lastStartIso,
  now,
}: SameDayWindowInput): boolean => {
  if (hourNow < hourLowerBound) {
    return false;
  }
  const lastStartMs = new Date(lastStartIso).getTime();
  return now.getTime() <= lastStartMs + POST_RACE_GRACE_MINUTES * MINUTE_MS;
};

const evaluateRows = (rows: PollingWindowRow[], now: Date): boolean => {
  const todayParts = getJstDateParts(now);
  const todayStr = toYyyyMmDd(todayParts);
  const tomorrowStr = toYyyyMmDd(tomorrowParts(now));
  const hourNow = Number(todayParts.hour);
  const jraToday = findRow(rows, JRA_SOURCE, todayStr);
  const jraTomorrow = findRow(rows, JRA_SOURCE, tomorrowStr);
  const narToday = findRow(rows, NAR_SOURCE, todayStr);
  const isJraTodayActive = jraToday
    ? isWithinSameDayWindow({
        hourLowerBound: JRA_BETTING_OPEN_TODAY_HOUR,
        hourNow,
        lastStartIso: jraToday.last_start,
        now,
      })
    : false;
  const isJraPrepActive = jraTomorrow ? hourNow >= JRA_BETTING_OPEN_PREP_HOUR : false;
  const isNarTodayActive = narToday
    ? isWithinSameDayWindow({
        hourLowerBound: NAR_BETTING_OPEN_TODAY_HOUR,
        hourNow,
        lastStartIso: narToday.last_start,
        now,
      })
    : false;
  return isJraTodayActive || isJraPrepActive || isNarTodayActive;
};

const readKvCachedDecision = async (env: Env): Promise<boolean | null> => {
  const cached = await env.ODDS_HOT_KV.get(POLLING_WINDOW_KV_KEY);
  if (cached === KV_VALUE_TRUE) {
    return true;
  }
  if (cached === KV_VALUE_FALSE) {
    return false;
  }
  return null;
};

const writeKvCachedDecision = async (env: Env, value: boolean): Promise<void> => {
  await env.ODDS_HOT_KV.put(POLLING_WINDOW_KV_KEY, value ? KV_VALUE_TRUE : KV_VALUE_FALSE, {
    expirationTtl: POLLING_WINDOW_KV_TTL_SECONDS,
  });
};

const queryWindowRows = async (env: Env, now: Date): Promise<PollingWindowRow[]> => {
  const todayStr = toYyyyMmDd(getJstDateParts(now));
  const tomorrowStr = toYyyyMmDd(tomorrowParts(now));
  const result = await env.REALTIME_HOT_DB.prepare(QUERY_RACE_WINDOW)
    .bind(todayStr, tomorrowStr)
    .all<PollingWindowRow>();
  return result.results;
};

export const shouldRunOddsCron = async (env: Env, now: Date): Promise<boolean> => {
  const cached = await readKvCachedDecision(env);
  if (cached !== null) {
    return cached;
  }
  const rows = await queryWindowRows(env, now);
  const active = evaluateRows(rows, now);
  await writeKvCachedDecision(env, active);
  return active;
};
