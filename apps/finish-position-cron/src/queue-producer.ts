// Run with bun. Enqueues per-category predict messages onto PREDICT_QUEUE.

import type { Env, PredictCategory, PredictMode, PredictQueueMessage } from "./types";

const ALL_CATEGORIES: PredictCategory[] = ["jra", "nar", "ban-ei"];

interface EnqueuePredictParams {
  env: Env;
  runDate: string;
  runYmd: string;
  daysAhead: number;
  mode: PredictMode;
  category?: PredictCategory;
  // Per-race rescore targeting. Present only when the trigger carries a single
  // category plus an explicit race; attached to every message this call sends.
  // Absent on the legacy per-category path, keeping those messages unchanged.
  keibajoCode?: string;
  raceBango?: string;
  skipDedup?: boolean;
}

// Spread the per-race target only when both fields are defined so the
// `satisfies PredictQueueMessage` typing stays exact and per-category messages
// keep their original shape (no undefined keibajoCode/raceBango keys).
const buildPerRaceTarget = (
  params: EnqueuePredictParams,
): Pick<PredictQueueMessage, "keibajoCode" | "raceBango"> =>
  params.keibajoCode !== undefined && params.raceBango !== undefined
    ? { keibajoCode: params.keibajoCode, raceBango: params.raceBango }
    : {};

export const enqueuePredict = async (params: EnqueuePredictParams): Promise<PredictCategory[]> => {
  const categories = params.category ? [params.category] : ALL_CATEGORIES;
  const perRaceTarget = buildPerRaceTarget(params);
  await Promise.all(
    categories.map((cat) =>
      params.env.PREDICT_QUEUE.send({
        category: cat,
        daysAhead: params.daysAhead,
        mode: params.mode,
        runDate: params.runDate,
        runDateIso: params.runDate,
        runYmd: params.runYmd,
        ...perRaceTarget,
        ...(params.skipDedup ? { skipDedup: true } : {}),
      } satisfies PredictQueueMessage),
    ),
  );
  return categories;
};
