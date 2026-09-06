// Run with bun. Enqueues per-category predict messages onto PREDICT_QUEUE.
// Production generation is per-race only: both keibajoCode and raceBango are
// required on every enqueue (see per-race-scope-guard.ts).

import { buildDayBaseObjectKey } from "./day-base-object-key";
import { recordDeliveryDetected, recordDeliveryEnqueued } from "./delivery-lifecycle";
import {
  failFocusedFullRaceEnqueue,
  reserveFocusedFullRaceEnqueue,
  reserveFocusedFullRaceRepair,
} from "./do-state";
import {
  isFocusedFullPredictionComplete,
  isPerRaceFeatureCachePresent,
} from "./focused-full-completion";
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

interface ExistingPredictionFreshParams {
  category: PredictCategory;
  params: EnqueuePredictParams & { keibajoCode: string; raceBango: string };
}

const currentRunningStyleGeneration = async (
  params: ExistingPredictionFreshParams,
): Promise<string | null> => {
  const canonical = await params.params.env.FEATURES_CACHE.head(
    buildDayBaseObjectKey({ category: params.category, runYmd: params.params.runYmd }),
  );
  const value = canonical?.customMetadata?.["rs-predicted-at-max"];
  return typeof value === "string" && Number.isFinite(Date.parse(value)) ? value : null;
};

const isExistingPredictionFresh = async (
  params: ExistingPredictionFreshParams,
): Promise<boolean> => {
  const featureCachePresent = await isPerRaceFeatureCachePresent({
    category: params.category,
    env: params.params.env,
    keibajoCode: params.params.keibajoCode,
    raceBango: params.params.raceBango,
    runYmd: params.params.runYmd,
  });
  if (!featureCachePresent) return false;
  if (params.category === "ban-ei") return true;
  const notBefore = await currentRunningStyleGeneration(params);
  if (notBefore === null) return false;
  return isFocusedFullPredictionComplete({
    category: params.category,
    env: params.params.env,
    keibajoCode: params.params.keibajoCode,
    notBefore,
    raceBango: params.params.raceBango,
    runYmd: params.params.runYmd,
  });
};

const enqueueCategory = async (input: EnqueueCategoryParams): Promise<boolean> => {
  const { category, params } = input;
  const now = new Date();
  const reservationId =
    params.mode === "full" && params.skipDedup === true && params.force !== true
      ? crypto.randomUUID()
      : undefined;
  if (reservationId !== undefined) {
    const reservationParams = {
      category,
      env: params.env,
      keibajoCode: params.keibajoCode,
      raceBango: params.raceBango,
      raceStartAtJst: params.raceStartAtJst,
      reservationId,
      runYmd: params.runYmd,
      staleAfterMs: FOCUSED_FULL_ENQUEUE_RESERVATION_STALE_MS,
    };
    let reservation = await reserveFocusedFullRaceEnqueue(reservationParams);
    // A final canonical may land after an earlier race attempt failed or after
    // an old prediction was marked success. Reopen both terminal success and
    // a stranded started lane when the exact-race feature cache is absent or
    // the KV/Neon prediction generation predates the canonical RS watermark.
    // This makes one final day-base fanout repair late-data races without
    // blindly duplicating races that already match the current generation.
    if (
      !reservation.proceed &&
      (reservation.state === "success" || reservation.state === "started") &&
      !(await isExistingPredictionFresh({ category, params }))
    ) {
      const reopenState = reservation.state;
      reservation = await reserveFocusedFullRaceRepair({
        category,
        env: params.env,
        keibajoCode: params.keibajoCode,
        raceBango: params.raceBango,
        raceStartAtJst: params.raceStartAtJst,
        reservationId,
        runYmd: params.runYmd,
        staleAfterMs: FOCUSED_FULL_ENQUEUE_RESERVATION_STALE_MS,
      });
      if (reservation.proceed) {
        console.warn(
          `[predict-producer] reopened stale focused-full ${reopenState} category=${category} runYmd=${params.runYmd} keibajo=${params.keibajoCode} race=${params.raceBango}`,
        );
      }
    }
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
    ...(params.force ? { force: true, forceRequestedAt: now.toISOString() } : {}),
    ...(params.deliveryTrackingId ? { deliveryTrackingId: params.deliveryTrackingId } : {}),
  } satisfies PredictQueueMessage;
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
