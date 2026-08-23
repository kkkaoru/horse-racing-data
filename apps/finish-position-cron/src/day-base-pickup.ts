// Run with bun. Delayed FEATURES_CACHE pickup after a detached day-base prewarm.
//
// /prewarm-day-base returns accepted as soon as DAY_CHAIN starts. An immediate
// GET /prewarm-day-base-cache then reads an empty in-process store, so R2 stays
// missing and every later race logs r2-missing-object. This module re-enqueues
// pickup after the first-day build (10-15m) has had time to commit.

import { pickUpPrewarmDayBase } from "./day-base-prewarm-pickup";
import { handOffContainerStopOrCleanup } from "./container-cleanup";
import { fanOutPredictionsAfterDayBaseHit } from "./feature-hit-prediction";
import { getFocusedFullDayBaseReadiness } from "./focused-full-day-base-readiness";
import { PREDICT_DO_NAME_PREFIX } from "./predict-do-shard";
import type { DayBasePickupMessage, Env, PredictCategory } from "./types";

interface EnqueueDayBasePickupParams {
  attempt: number;
  category: PredictCategory;
  env: Env;
  runYmd: string;
  generatePredictionsAfterHit?: boolean;
  force?: boolean;
}

interface CleanupDayBaseWorkParams {
  category: PredictCategory;
  env: Env;
  runYmd: string;
}

interface ConsumeDayBasePickupParams {
  env: Env;
  message: DayBasePickupMessage;
}

export const DAY_BASE_PICKUP_TYPE = "day-base-pickup";
export const DAY_BASE_PICKUP_DELAY_SECONDS = 180;
// The Python day-base pipeline has a 30-minute deadline. Eleven 3-minute
// pickup polls cover 33 minutes, so the Worker can observe either the final
// object or the authoritative pipeline timeout before releasing its slot.
export const DAY_BASE_PICKUP_MAX_ATTEMPTS = 11;
export const DAY_BASE_PICKUP_FIRST_ATTEMPT = 1;

export const buildDayBaseWorkKey = (category: PredictCategory, runYmd: string): string =>
  `day-base:${runYmd}:${category}`;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

export const isDayBasePickupMessage = (value: unknown): value is DayBasePickupMessage => {
  if (!isRecord(value)) return false;
  if (value.type !== DAY_BASE_PICKUP_TYPE) return false;
  if (typeof value.category !== "string" || value.category.length === 0) return false;
  if (typeof value.runYmd !== "string" || value.runYmd.length === 0) return false;
  if (
    value.generatePredictionsAfterHit !== undefined &&
    typeof value.generatePredictionsAfterHit !== "boolean"
  )
    return false;
  if (value.force !== undefined && typeof value.force !== "boolean") return false;
  return typeof value.attempt === "number" && Number.isInteger(value.attempt) && value.attempt > 0;
};

export const isDayBasePickupQueueMessage = (
  message: Message<unknown>,
): message is Message<DayBasePickupMessage> => isDayBasePickupMessage(message.body);

export const enqueueDayBasePickup = async (params: EnqueueDayBasePickupParams): Promise<void> => {
  const message: DayBasePickupMessage = {
    attempt: params.attempt,
    category: params.category,
    runYmd: params.runYmd,
    type: DAY_BASE_PICKUP_TYPE,
    ...(params.generatePredictionsAfterHit ? { generatePredictionsAfterHit: true } : {}),
    ...(params.force === true ? { force: true } : {}),
  };
  await params.env.PREDICT_QUEUE.send(message, { delaySeconds: DAY_BASE_PICKUP_DELAY_SECONDS });
  console.log(
    `[day-base-pickup] scheduled category=${params.category} runYmd=${params.runYmd} attempt=${params.attempt} delaySeconds=${DAY_BASE_PICKUP_DELAY_SECONDS}`,
  );
};

export const cleanupDayBaseWork = (params: CleanupDayBaseWorkParams): Promise<void> =>
  handOffContainerStopOrCleanup({
    env: params.env,
    name: `${PREDICT_DO_NAME_PREFIX}${params.category}`,
    role: "legacy",
    workKey: buildDayBaseWorkKey(params.category, params.runYmd),
  });

export const consumeDayBasePickup = async (params: ConsumeDayBasePickupParams): Promise<void> => {
  const { env, message } = params;
  const { category, runYmd, attempt } = message;
  // A delayed pickup may arrive after another pickup already committed the
  // same live generation and stopped its Container.  Fetching the DO first in
  // that case cold-starts a standard-4 solely to discover that its in-process
  // cache is empty.  Canonical R2 readiness is sufficient idempotency proof:
  // it compares the object metadata with the current Catalog/RS watermarks,
  // so a genuinely newer generation still fails this fast path and continues
  // to the Container pickup below.
  const existingReadiness = await getFocusedFullDayBaseReadiness({ category, env, runYmd }).catch(
    () => ({ ready: false, reason: "readiness-error" }),
  );
  if (existingReadiness.ready) {
    console.log(
      `[day-base-pickup] already-landed category=${category} runYmd=${runYmd} attempt=${attempt}`,
    );
    if (message.generatePredictionsAfterHit === true) {
      await fanOutPredictionsAfterDayBaseHit({ category, env, runYmd });
    }
    await cleanupDayBaseWork({ category, env, runYmd });
    return;
  }
  // An old R2 object can still be present while the detached build is
  // producing a new watermark. Only a successful container pickup may prove
  // this generation landed; presence alone must not acknowledge the message.
  const picked = await pickUpPrewarmDayBase({ category, env, runYmd });
  const readiness = picked
    ? await getFocusedFullDayBaseReadiness({ category, env, runYmd }).catch((error) => {
        console.error(
          `[day-base-pickup] canonical readiness failed category=${category} runYmd=${runYmd}: ${String(error)}`,
        );
        return { ready: false, reason: "readiness-error" };
      })
    : { ready: false, reason: "pickup-missing" };
  if (picked && readiness.ready) {
    console.log(
      `[day-base-pickup] landed category=${category} runYmd=${runYmd} attempt=${attempt}`,
    );
    if (message.generatePredictionsAfterHit === true) {
      await fanOutPredictionsAfterDayBaseHit({ category, env, runYmd });
    }
    await cleanupDayBaseWork({ category, env, runYmd });
    return;
  }
  if (picked) {
    console.warn(
      `[day-base-pickup] rejected stale pickup category=${category} runYmd=${runYmd} attempt=${attempt} reason=${readiness.reason}`,
    );
  }
  if (attempt >= DAY_BASE_PICKUP_MAX_ATTEMPTS) {
    console.warn(
      `[day-base-pickup] exhausted category=${category} runYmd=${runYmd} attempt=${attempt}`,
    );
    await cleanupDayBaseWork({ category, env, runYmd });
    return;
  }
  await enqueueDayBasePickup({
    attempt: attempt + 1,
    category,
    env,
    ...(message.generatePredictionsAfterHit === true ? { generatePredictionsAfterHit: true } : {}),
    ...(message.force === true ? { force: true } : {}),
    runYmd,
  });
};
