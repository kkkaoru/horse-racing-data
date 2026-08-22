// Run with bun. Delayed FEATURES_CACHE pickup after a detached day-base prewarm.
//
// /prewarm-day-base returns accepted as soon as DAY_CHAIN starts. An immediate
// GET /prewarm-day-base-cache then reads an empty in-process store, so R2 stays
// missing and every later race logs r2-missing-object. This module re-enqueues
// pickup after the first-day build (10-15m) has had time to commit.

import { headDayBaseObject, pickUpPrewarmDayBase } from "./day-base-prewarm-pickup";
import { enqueueContainerStop } from "./container-control";
import { releaseContainerSlot } from "./do-state";
import { fanOutPredictionsAfterDayBaseHit } from "./feature-hit-prediction";
import { PREDICT_DO_NAME_PREFIX } from "./predict-do-shard";
import type { DayBasePickupMessage, Env, PredictCategory } from "./types";

interface EnqueueDayBasePickupParams {
  attempt: number;
  category: PredictCategory;
  env: Env;
  runYmd: string;
  generatePredictionsAfterHit?: boolean;
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
const DAY_BASE_SLOT_KIND = "day-base";

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
  };
  await params.env.PREDICT_QUEUE.send(message, { delaySeconds: DAY_BASE_PICKUP_DELAY_SECONDS });
  console.log(
    `[day-base-pickup] scheduled category=${params.category} runYmd=${params.runYmd} attempt=${params.attempt} delaySeconds=${DAY_BASE_PICKUP_DELAY_SECONDS}`,
  );
};

const releaseDayBasePickupSlot = async (
  env: Env,
  category: PredictCategory,
  runYmd: string,
): Promise<void> => {
  const doName = `${PREDICT_DO_NAME_PREFIX}${category}`;
  const workKey = buildDayBaseWorkKey(category, runYmd);
  try {
    if (await enqueueContainerStop(env, doName, workKey)) return;
    await releaseContainerSlot({
      doName,
      env,
      kind: DAY_BASE_SLOT_KIND,
      workKey,
    });
  } catch (error) {
    console.error(
      `[day-base-pickup] failed to hand off container stop category=${category} runYmd=${runYmd}: ${String(error)}`,
    );
    try {
      await releaseContainerSlot({ doName, env, kind: DAY_BASE_SLOT_KIND, workKey });
    } catch (releaseError) {
      console.error(
        `[day-base-pickup] failed to release slot category=${category} runYmd=${runYmd}: ${String(releaseError)}`,
      );
    }
  }
};

export const consumeDayBasePickup = async (params: ConsumeDayBasePickupParams): Promise<void> => {
  const { env, message } = params;
  const { category, runYmd, attempt } = message;
  // An old R2 object can still be present while the detached build is
  // producing a new watermark. Only a successful container pickup may prove
  // this generation landed; presence alone must not acknowledge the message.
  const picked = await pickUpPrewarmDayBase({ category, env, runYmd });
  if (picked && (await headDayBaseObject({ category, env, runYmd })) !== null) {
    console.log(
      `[day-base-pickup] landed category=${category} runYmd=${runYmd} attempt=${attempt}`,
    );
    if (message.generatePredictionsAfterHit === true) {
      await fanOutPredictionsAfterDayBaseHit({ category, env, runYmd });
    }
    await releaseDayBasePickupSlot(env, category, runYmd);
    return;
  }
  if (attempt >= DAY_BASE_PICKUP_MAX_ATTEMPTS) {
    console.warn(
      `[day-base-pickup] exhausted category=${category} runYmd=${runYmd} attempt=${attempt}`,
    );
    await releaseDayBasePickupSlot(env, category, runYmd);
    return;
  }
  await enqueueDayBasePickup({
    attempt: attempt + 1,
    category,
    env,
    ...(message.generatePredictionsAfterHit === true ? { generatePredictionsAfterHit: true } : {}),
    runYmd,
  });
};
