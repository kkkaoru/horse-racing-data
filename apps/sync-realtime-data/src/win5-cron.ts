import { fetchWin5SchedulesFromJra } from "../../pc-keiba-viewer/src/lib/win5/jra-parse";
import type { Win5Schedule } from "../../pc-keiba-viewer/src/lib/win5/types";
import { formatError } from "./format-error";
import { getFinishPositionPool } from "./finish-position-lite-pool";
import { logFetch } from "./storage";
import type { Env, Win5ScheduleJob } from "./types";
import { getWin5Prediction, upsertWin5Schedule } from "./win5-d1";
import { enrichWin5ScheduleLegs } from "./win5-postgres";
import { formatTomorrowYYYYMMDDInJst, formatYYYYMMDDInJst } from "./running-style-cron";

export const WIN5_DISCOVER_CRON = "30 12 * * *";

const QUEUE_SEND_BATCH_SIZE = 100;
const ENABLED_FLAG = "1";

const isWin5Enabled = (env: Env): boolean => env.WIN5_D1_WRITE_ENABLED === ENABLED_FLAG;

const enqueueWin5Jobs = async (env: Env, jobs: Win5ScheduleJob[]): Promise<number> => {
  if (!env.WIN5_JOBS || jobs.length === 0) {
    return 0;
  }
  for (let index = 0; index < jobs.length; index += QUEUE_SEND_BATCH_SIZE) {
    await env.WIN5_JOBS.sendBatch(
      jobs.slice(index, index + QUEUE_SEND_BATCH_SIZE).map((body) => ({ body })),
    );
  }
  return jobs.length;
};

export const syncWin5SchedulesFromJra = async (
  env: Env,
  options?: { fallbackYear?: string; fetchedAt?: string },
): Promise<Win5Schedule[]> => {
  const fetchedAt = options?.fetchedAt ?? new Date().toISOString();
  const schedules = await fetchWin5SchedulesFromJra({
    fallbackYear: options?.fallbackYear,
    fetchedAt,
  });
  const pool = getFinishPositionPool(env);
  const enrichedSchedules: Win5Schedule[] = [];

  for (const schedule of schedules) {
    const enriched = await enrichWin5ScheduleLegs(pool, schedule);
    await upsertWin5Schedule(env.REALTIME_DB, enriched);
    enrichedSchedules.push(enriched);
  }

  return enrichedSchedules;
};

export const planWin5PredictionsForDate = async (
  env: Env,
  date: string,
  now = new Date(),
): Promise<{ date: string; enqueued: number; scanned: number }> => {
  if (!isWin5Enabled(env)) {
    return { date, enqueued: 0, scanned: 0 };
  }

  const kaisaiNen = date.slice(0, 4);
  const kaisaiTsukihi = date.slice(4, 8);
  const existing = await getWin5Prediction(
    env.REALTIME_DB,
    kaisaiNen,
    kaisaiTsukihi,
    "win5-heuristic-v1",
  );
  if (existing) {
    return { date, enqueued: 0, scanned: 1 };
  }

  const enqueued = await enqueueWin5Jobs(env, [
    {
      type: "generate-win5-predictions",
      kaisaiNen,
      kaisaiTsukihi,
      predictedAt: now.toISOString(),
    },
  ]);

  return { date, enqueued, scanned: 1 };
};

export const discoverWin5Schedules = async (
  env: Env,
  now = new Date(),
): Promise<{ discovered: number; enqueued: number }> => {
  if (!isWin5Enabled(env)) {
    return { discovered: 0, enqueued: 0 };
  }

  const schedules = await syncWin5SchedulesFromJra(env, {
    fallbackYear: formatYYYYMMDDInJst(now).slice(0, 4),
    fetchedAt: now.toISOString(),
  });

  const jobs: Win5ScheduleJob[] = schedules.map((schedule) => ({
    type: "generate-win5-predictions",
    kaisaiNen: schedule.kaisaiNen,
    kaisaiTsukihi: schedule.kaisaiTsukihi,
    predictedAt: now.toISOString(),
  }));
  const enqueued = await enqueueWin5Jobs(env, jobs);
  return { discovered: schedules.length, enqueued };
};

export const runWin5CronTick = async (
  env: Env,
  scheduledAt: Date,
): Promise<{ discovered: number; enqueued: number; tomorrowPlanned: number }> => {
  const discovery = await discoverWin5Schedules(env, scheduledAt);
  const tomorrow = formatTomorrowYYYYMMDDInJst(scheduledAt);
  const tomorrowPlan = await planWin5PredictionsForDate(env, tomorrow, scheduledAt);
  return {
    ...discovery,
    tomorrowPlanned: tomorrowPlan.enqueued,
  };
};

export const logWin5CronResult = async (env: Env, scheduledAt: Date): Promise<void> => {
  await runWin5CronTick(env, scheduledAt)
    .then((summary) =>
      logFetch(env.REALTIME_DB, "discover-win5-schedules", "ok", null, JSON.stringify(summary)),
    )
    .catch((error: unknown) =>
      logFetch(env.REALTIME_DB, "discover-win5-schedules", "error", null, formatError(error)),
    );
};
