// Run with bun. Fan out the first per-race prediction only after the category's
// fresh day-base object has been confirmed as a cache HIT.

import { enumerateTodaysRaces } from "./cron-decision";
import { enqueuePredict } from "./queue-producer";
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

export const fanOutPredictionsAfterDayBaseHit = async (
  params: FanOutPredictionsAfterDayBaseHitParams,
): Promise<number> => {
  const races = await enumerateTodaysRaces(params.env.REALTIME_DB, params.runYmd);
  const categoryRaces = races.filter((race) => race.category === params.category);
  const runDate = buildRunDate(params.runYmd);
  const enqueueResults: PredictCategory[][] = [];
  // enumerateTodaysRaces is ordered by post time. Send sequentially so the
  // Queue observes the same order instead of letting concurrent network
  // completion reorder later races ahead of today's early starters.
  for (const race of categoryRaces) {
    enqueueResults.push(
      await enqueuePredict({
        category: params.category,
        daysAhead: Number(params.env.PREDICT_DAYS_AHEAD),
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
    `[feature-hit-prediction] enqueued category=${params.category} runYmd=${params.runYmd} races=${enqueuedCount} duplicates=${categoryRaces.length - enqueuedCount}`,
  );
  return enqueuedCount;
};
