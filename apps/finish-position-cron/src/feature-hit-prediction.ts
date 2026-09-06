// Run with bun. Fan out the first per-race prediction only after the category's
// fresh day-base object has been confirmed as a cache HIT.

import { enumerateTodaysRaces } from "./cron-decision";
import { enqueuePredict } from "./queue-producer";
import { warmNeon } from "./neon-warm";
import { getRunningStyleRaceReadiness } from "./running-style-readiness";
import { getRunYmdJst } from "./time";
import type { Env, PredictCategory } from "./types";

interface FanOutPredictionsAfterDayBaseHitParams {
  category: PredictCategory;
  env: Env;
  runYmd: string;
}

const RUN_YMD_YEAR_END = 4;
const RUN_YMD_MONTH_END = 6;

const buildRunDate = (runYmd: string): string =>
  `${runYmd.slice(0, RUN_YMD_YEAR_END)}-${runYmd.slice(
    RUN_YMD_YEAR_END,
    RUN_YMD_MONTH_END,
  )}-${runYmd.slice(RUN_YMD_MONTH_END)}`;

const resolveDaysAhead = (runYmd: string, configuredDaysAhead: string): number =>
  runYmd === getRunYmdJst(new Date()) ? 0 : Number(configuredDaysAhead);

export const fanOutPredictionsAfterDayBaseHit = async (
  params: FanOutPredictionsAfterDayBaseHitParams,
): Promise<number> => {
  const races = await enumerateTodaysRaces(params.env.REALTIME_DB, params.runYmd);
  // Late source delivery must still produce predictions and Viewer cache for
  // every scheduled race. Post time is display metadata, not a completion
  // fence: skipping started races made a delayed daily run report success
  // while most of that day's prediction and heatmap cache remained absent.
  const categoryRaces = races.filter((race) => race.category === params.category);
  const runningStyle =
    params.category === "ban-ei"
      ? categoryRaces.map((race) => ({ race, reason: null }))
      : await getRunningStyleRaceReadiness({
          category: params.category,
          db: params.env.REALTIME_DB,
          races: categoryRaces,
          runYmd: params.runYmd,
        });
  const incomplete = runningStyle.filter((race) => race.reason !== null);
  if (incomplete.length > 0) {
    throw new Error(
      `Running-style barrier incomplete category=${params.category} runYmd=${params.runYmd} ready=${String(categoryRaces.length - incomplete.length)} expected=${String(categoryRaces.length)}`,
    );
  }
  const readyRaces = categoryRaces;
  const runDate = buildRunDate(params.runYmd);
  const daysAhead = resolveDaysAhead(params.runYmd, params.env.PREDICT_DAYS_AHEAD);
  const enqueueResults: PredictCategory[][] = [];
  // A day-base HIT means the feature parquet is ready, but the first focused
  // prediction still reads Neon for the race-scoped rows. Wake Neon before
  // handing the first pre-weight message to Queue so the Container does not
  // spend its critical path waiting for database compute activation. This is
  // best-effort and never suppresses a valid enqueue.
  if (params.env.NEON_DATABASE_URL !== undefined) {
    await warmNeon(params.env.NEON_DATABASE_URL);
  }
  // enumerateTodaysRaces is ordered by post time. Send sequentially so the
  // Queue observes the same order instead of letting concurrent network
  // completion reorder later races ahead of today's early starters.
  for (const race of readyRaces) {
    enqueueResults.push(
      await enqueuePredict({
        category: params.category,
        daysAhead,
        env: params.env,
        keibajoCode: race.keibajoCode,
        mode: "full",
        raceBango: race.raceBango,
        raceStartAtJst: race.raceStartAtJst,
        runDate,
        runYmd: params.runYmd,
        skipDedup: true,
      }),
    );
  }
  const enqueuedCount = enqueueResults.flat().length;
  console.log(
    `[feature-hit-prediction] enqueued category=${params.category} runYmd=${params.runYmd} races=${enqueuedCount} duplicates=${readyRaces.length - enqueuedCount}`,
  );
  return enqueuedCount;
};
