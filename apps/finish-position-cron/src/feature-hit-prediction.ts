// Run with bun. Fan out the first per-race prediction only after the category's
// fresh day-base object has been confirmed as a cache HIT.

import { enumerateTodaysRaces } from "./cron-decision";
import { enqueuePredict } from "./queue-producer";
import { getRunningStyleRaceReadiness } from "./running-style-readiness";
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
  const readiness = await getRunningStyleRaceReadiness({
    category: params.category,
    db: params.env.REALTIME_DB,
    races: categoryRaces,
    runYmd: params.runYmd,
  });
  const readyRaces = readiness.filter((item) => item.reason === null).map((item) => item.race);
  readiness
    .filter((item) => item.reason !== null)
    .forEach((item) =>
      console.warn(
        `[feature-hit-prediction] skipped-running-style-incomplete category=${params.category} runYmd=${params.runYmd} keibajoCode=${item.race.keibajoCode} raceBango=${item.race.raceBango} reason=${item.reason}`,
      ),
    );
  const runDate = buildRunDate(params.runYmd);
  const enqueueResults: PredictCategory[][] = [];
  // enumerateTodaysRaces is ordered by post time. Send sequentially so the
  // Queue observes the same order instead of letting concurrent network
  // completion reorder later races ahead of today's early starters.
  for (const race of readyRaces) {
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
    `[feature-hit-prediction] enqueued category=${params.category} runYmd=${params.runYmd} races=${enqueuedCount} duplicates=${readyRaces.length - enqueuedCount} runningStyleIncomplete=${categoryRaces.length - readyRaces.length}`,
  );
  return enqueuedCount;
};
