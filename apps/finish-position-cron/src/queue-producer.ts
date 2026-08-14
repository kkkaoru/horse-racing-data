// Run with bun. Enqueues per-category predict messages onto PREDICT_QUEUE.
// Production generation is per-race only: both keibajoCode and raceBango are
// required on every enqueue (see per-race-scope-guard.ts).

import { recordDeliveryDetected, recordDeliveryEnqueued } from "./delivery-lifecycle";
import { hasRequiredPerRaceScope, PER_RACE_SCOPE_REQUIRED_ERROR } from "./per-race-scope-guard";
import type { Env, PredictCategory, PredictMode, PredictQueueMessage } from "./types";

const ALL_CATEGORIES: PredictCategory[] = ["jra", "nar", "ban-ei"];

interface EnqueuePredictParams {
  env: Env;
  runDate: string;
  runYmd: string;
  daysAhead: number;
  mode: PredictMode;
  category?: PredictCategory;
  // Required per-race target. Both fields must be present -- day-scoped
  // ("all") enqueues are rejected by hasRequiredPerRaceScope below.
  keibajoCode?: string;
  raceBango?: string;
  skipDedup?: boolean;
  debug?: boolean;
  force?: boolean;
  deliveryTrackingId?: string;
}

export const enqueuePredict = async (params: EnqueuePredictParams): Promise<PredictCategory[]> => {
  if (!hasRequiredPerRaceScope(params)) {
    throw new Error(PER_RACE_SCOPE_REQUIRED_ERROR);
  }
  const categories = params.category ? [params.category] : ALL_CATEGORIES;
  for (const cat of categories) {
    const message = {
      category: cat,
      daysAhead: params.daysAhead,
      keibajoCode: params.keibajoCode,
      mode: params.mode,
      raceBango: params.raceBango,
      runDate: params.runDate,
      runDateIso: params.runDate,
      runYmd: params.runYmd,
      ...(params.skipDedup ? { skipDedup: true } : {}),
      ...(params.debug ? { debug: true } : {}),
      ...(params.force ? { force: true } : {}),
      ...(params.deliveryTrackingId ? { deliveryTrackingId: params.deliveryTrackingId } : {}),
    } satisfies PredictQueueMessage;
    const now = new Date();
    await recordDeliveryDetected(params.env, message, now);
    await params.env.PREDICT_QUEUE.send(message);
    await recordDeliveryEnqueued(params.env, message, new Date());
  }
  return categories;
};
