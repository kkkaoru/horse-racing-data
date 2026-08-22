// Run with bun. Enqueues per-category predict messages onto PREDICT_QUEUE.
// Production generation is per-race only: both keibajoCode and raceBango are
// required on every enqueue (see per-race-scope-guard.ts).

import { recordDeliveryDetected, recordDeliveryEnqueued } from "./delivery-lifecycle";
import { failFocusedFullRaceEnqueue, reserveFocusedFullRaceEnqueue } from "./do-state";
import { hasRequiredPerRaceScope, PER_RACE_SCOPE_REQUIRED_ERROR } from "./per-race-scope-guard";
import type { Env, PredictCategory, PredictMode, PredictQueueMessage } from "./types";

const ALL_CATEGORIES: PredictCategory[] = ["jra", "nar", "ban-ei"];
const FOCUSED_FULL_ENQUEUE_RESERVATION_STALE_MS = 31 * 60 * 1000;

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
  raceStartAtJst?: string;
  skipDedup?: boolean;
  debug?: boolean;
  force?: boolean;
  deliveryTrackingId?: string;
}

interface EnqueueCategoryParams {
  category: PredictCategory;
  params: EnqueuePredictParams & { keibajoCode: string; raceBango: string };
}

const enqueueCategory = async (input: EnqueueCategoryParams): Promise<boolean> => {
  const { category, params } = input;
  const reservationId =
    params.mode === "full" && params.skipDedup === true && params.force !== true
      ? crypto.randomUUID()
      : undefined;
  if (reservationId !== undefined) {
    const reservation = await reserveFocusedFullRaceEnqueue({
      category,
      env: params.env,
      keibajoCode: params.keibajoCode,
      raceBango: params.raceBango,
      raceStartAtJst: params.raceStartAtJst,
      reservationId,
      runYmd: params.runYmd,
      staleAfterMs: FOCUSED_FULL_ENQUEUE_RESERVATION_STALE_MS,
    });
    if (!reservation.proceed) return false;
  }
  const message = {
    category,
    daysAhead: params.daysAhead,
    keibajoCode: params.keibajoCode,
    mode: params.mode,
    raceBango: params.raceBango,
    ...(params.raceStartAtJst ? { raceStartAtJst: params.raceStartAtJst } : {}),
    runDate: params.runDate,
    runDateIso: params.runDate,
    runYmd: params.runYmd,
    ...(params.skipDedup ? { skipDedup: true } : {}),
    ...(params.debug ? { debug: true } : {}),
    ...(params.force ? { force: true } : {}),
    ...(params.deliveryTrackingId ? { deliveryTrackingId: params.deliveryTrackingId } : {}),
  } satisfies PredictQueueMessage;
  const now = new Date();
  try {
    await recordDeliveryDetected(params.env, message, now);
    await params.env.PREDICT_QUEUE.send(message);
  } catch (error) {
    if (reservationId !== undefined) {
      try {
        await failFocusedFullRaceEnqueue({
          category,
          env: params.env,
          keibajoCode: params.keibajoCode,
          raceBango: params.raceBango,
          reservationId,
          runYmd: params.runYmd,
        });
      } catch (releaseError) {
        console.error("Failed to release focused-full enqueue reservation:", String(releaseError));
      }
    }
    throw error;
  }
  try {
    await recordDeliveryEnqueued(params.env, message, new Date());
  } catch (error) {
    console.error("Failed to record enqueued prediction delivery:", String(error));
  }
  return true;
};

export const enqueuePredict = async (params: EnqueuePredictParams): Promise<PredictCategory[]> => {
  if (!hasRequiredPerRaceScope(params)) {
    throw new Error(PER_RACE_SCOPE_REQUIRED_ERROR);
  }
  const categories = params.category ? [params.category] : ALL_CATEGORIES;
  const results = await Promise.all(
    categories.map(async (category) => ({
      category,
      enqueued: await enqueueCategory({ category, params }),
    })),
  );
  return results.filter((result) => result.enqueued).map((result) => result.category);
};
