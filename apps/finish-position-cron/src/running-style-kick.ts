// Run with bun. Queue kick of sync-realtime-data's plan-running-style-predictions
// job for the JST windows sync-realtime-data's native running-style crons do not
// cover.
//
// sync-realtime-data (read-only reference -- this Worker does not edit that
// package) already runs two native running-style crons of its own
// (apps/sync-realtime-data/src/running-style-cron.ts):
//   - RUNNING_STYLE_INFERENCE_CRON = "*/10 0-14 * * *" (every 10 min, UTC
//     00:00-14:59 == JST 09:00-23:59) -- plans TODAY's races.
//   - RUNNING_STYLE_PREWARM_CRON = "0 12 * * *" (UTC 12:00 == JST 21:00, once
//     daily) -- plans TOMORROW's races.
// Neither fires during JST 00:00-08:59 (UTC 15:00-23:59 the previous UTC day)
// for TODAY -- exactly the gap scripts/launchd/race-prediction-guard.sh's JST
// 0-9 hourly band exists to fill today, by submitting the same job from a Mac
// launchd timer (USER directive 2026-07-18: production prediction generation
// must never depend on Mac-based batch processing). This module reproduces that
// gap-filling kick as a Cloudflare Cron Trigger owned by THIS Worker, using a
// direct producer binding to sync-realtime-data-jobs. It never needs the public
// /api/jobs endpoint, an Access policy, or a bearer token.
//
// A second cron below re-kicks TOMORROW's prewarm at JST 22:00 and 23:00 (UTC
// 13:00, 14:00), matching the guard's own 21/22/23 hourly tomorrow band:
// RUNNING_STYLE_PREWARM_CRON only fires once (JST 21:00), so if tomorrow's
// race data was still incomplete in D1 at that instant, no further attempt
// happens on the sync-realtime-data side until the next day's 21:00. These two
// extra attempts give the same redundancy the guard's repeated hourly kicks
// provided.
//
// Idempotent: planRunningStylePredictionsForDate only enqueues races that are
// still missing predictions (see selectRacesNeedingRunningStyleInference in
// running-style-cron.ts), so firing this cron alongside the still-running Mac
// guard during the transition is safe -- a redundant kick just sees "already
// queued/complete" and enqueues nothing extra.

import { getRunYmdJst } from "./time";
import type { RunningStylePlanJobMessage } from "./types";

const RS_KICK_JOB_TYPE = "plan-running-style-predictions";
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const YYYYMMDD_YEAR_END = 4;
const YYYYMMDD_MONTH_END = 6;
const YYYYMMDD_LENGTH = 8;
const TOMORROW_OFFSET_DAYS = 1;
const DATE_PAD_WIDTH = 2;
const PAD_CHAR = "0";
const MISSING_QUEUE_ERROR = "RUNNING_STYLE_PLAN_JOBS binding is unavailable";

// Hourly, UTC 15:00-23:00 == JST 00:00-08:00 (9 fires). JST 09:00 (UTC 00:00)
// is already covered by sync-realtime-data's RUNNING_STYLE_INFERENCE_CRON, so
// this range deliberately stops one hour short of it rather than overlapping.
export const RUNNING_STYLE_KICK_CRON_MORNING_GAP = "0 15-23 * * *";

// UTC 13:00, 14:00 == JST 22:00, 23:00 (2 fires) -- redundant re-attempts for
// TOMORROW after sync-realtime-data's own RUNNING_STYLE_PREWARM_CRON (UTC
// 12:00 == JST 21:00).
export const RUNNING_STYLE_KICK_CRON_TOMORROW_PREWARM = "0 13,14 * * *";

const MORNING_GAP_CRONS: ReadonlySet<string> = new Set([RUNNING_STYLE_KICK_CRON_MORNING_GAP]);
const TOMORROW_PREWARM_CRONS: ReadonlySet<string> = new Set([
  RUNNING_STYLE_KICK_CRON_TOMORROW_PREWARM,
]);

interface KickRunningStylePlanParams {
  date: string;
  env: RunningStyleKickEnv;
}

interface RunRunningStyleKickParams {
  env: RunningStyleKickEnv;
  now: Date;
}

interface RunningStyleKickEnv {
  RUNNING_STYLE_PLAN_JOBS?: RunningStyleKickQueue;
}

interface RunningStyleKickQueue {
  send(message: RunningStylePlanJobMessage): Promise<unknown>;
}

// Returns true when the cron string matches the TODAY morning-gap schedule.
export const shouldRunRunningStyleKickMorningGapCron = (cron: string): boolean =>
  MORNING_GAP_CRONS.has(cron);

// Returns true when the cron string matches the TOMORROW prewarm-redundancy schedule.
export const shouldRunRunningStyleKickTomorrowPrewarmCron = (cron: string): boolean =>
  TOMORROW_PREWARM_CRONS.has(cron);

// Duplicated (not imported) from corner-features-refresh.ts's own private copy
// of the same date-math -- that module's own docstring establishes the
// precedent this repeats: "a self-contained module owns its own cron string
// rather than centralizing every schedule in one file"; the same reasoning
// applies to this small date-shift helper.
const addDaysToYyyymmdd = (yyyymmdd: string, days: number): string => {
  const year = Number.parseInt(yyyymmdd.slice(0, YYYYMMDD_YEAR_END), 10);
  const month = Number.parseInt(yyyymmdd.slice(YYYYMMDD_YEAR_END, YYYYMMDD_MONTH_END), 10) - 1;
  const day = Number.parseInt(yyyymmdd.slice(YYYYMMDD_MONTH_END, YYYYMMDD_LENGTH), 10);
  const shifted = new Date(Date.UTC(year, month, day) + days * MS_PER_DAY);
  const y = shifted.getUTCFullYear();
  const m = String(shifted.getUTCMonth() + 1).padStart(DATE_PAD_WIDTH, PAD_CHAR);
  const d = String(shifted.getUTCDate()).padStart(DATE_PAD_WIDTH, PAD_CHAR);
  return `${y}${m}${d}`;
};

// Queue failures are logged with their target date and rethrown so the Cron
// execution is observable as failed instead of silently leaving the gap open.
export const kickRunningStylePlan = async (params: KickRunningStylePlanParams): Promise<void> => {
  const { date, env } = params;
  console.log(`[running-style-kick] start date=${date}`);
  try {
    if (!env.RUNNING_STYLE_PLAN_JOBS) {
      throw new Error(MISSING_QUEUE_ERROR);
    }
    await env.RUNNING_STYLE_PLAN_JOBS.send({ date, type: RS_KICK_JOB_TYPE });
    console.log(`[running-style-kick] queued date=${date}`);
  } catch (err) {
    console.error(`[running-style-kick] failed date=${date}: ${String(err)}`);
    throw err;
  }
};

// TODAY's morning-gap kick (RUNNING_STYLE_KICK_CRON_MORNING_GAP above).
export const runRunningStyleKickMorningGap = async (
  params: RunRunningStyleKickParams,
): Promise<void> => {
  const { env, now } = params;
  await kickRunningStylePlan({ date: getRunYmdJst(now), env });
};

// TOMORROW's prewarm-redundancy kick (RUNNING_STYLE_KICK_CRON_TOMORROW_PREWARM above).
export const runRunningStyleKickTomorrowPrewarm = async (
  params: RunRunningStyleKickParams,
): Promise<void> => {
  const { env, now } = params;
  const tomorrow = addDaysToYyyymmdd(getRunYmdJst(now), TOMORROW_OFFSET_DAYS);
  await kickRunningStylePlan({ date: tomorrow, env });
};
