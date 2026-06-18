// Run with bun. Enqueues per-category predict messages onto PREDICT_QUEUE.

import type { Env, PredictCategory, PredictQueueMessage } from "./types";

const ALL_CATEGORIES: PredictCategory[] = ["jra", "nar", "ban-ei"];

interface EnqueuePredictParams {
  env: Env;
  runDate: string;
  runYmd: string;
  daysAhead: number;
  category?: PredictCategory;
}

export const enqueuePredict = async (params: EnqueuePredictParams): Promise<PredictCategory[]> => {
  const categories = params.category ? [params.category] : ALL_CATEGORIES;
  await Promise.all(
    categories.map((cat) =>
      params.env.PREDICT_QUEUE.send({
        category: cat,
        daysAhead: params.daysAhead,
        runDate: params.runDate,
        runDateIso: params.runDate,
        runYmd: params.runYmd,
      } satisfies PredictQueueMessage),
    ),
  );
  return categories;
};
