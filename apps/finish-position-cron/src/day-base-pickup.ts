// Run with bun. Delayed FEATURES_CACHE pickup after a detached day-base prewarm.
//
// /prewarm-day-base returns accepted as soon as DAY_CHAIN starts. An immediate
// GET /prewarm-day-base-cache then reads an empty in-process store, so R2 stays
// missing and every later race logs r2-missing-object. This module re-enqueues
// pickup after the first-day build (10-15m) has had time to commit.

import { headDayBaseObject, pickUpPrewarmDayBase } from "./day-base-prewarm-pickup";
import type { DayBasePickupMessage, Env, PredictCategory } from "./types";

interface EnqueueDayBasePickupParams {
  attempt: number;
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
export const DAY_BASE_PICKUP_MAX_ATTEMPTS = 6;
export const DAY_BASE_PICKUP_FIRST_ATTEMPT = 1;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

export const isDayBasePickupMessage = (value: unknown): value is DayBasePickupMessage => {
  if (!isRecord(value)) return false;
  if (value.type !== DAY_BASE_PICKUP_TYPE) return false;
  if (typeof value.category !== "string" || value.category.length === 0) return false;
  if (typeof value.runYmd !== "string" || value.runYmd.length === 0) return false;
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
  };
  await params.env.PREDICT_QUEUE.send(message, { delaySeconds: DAY_BASE_PICKUP_DELAY_SECONDS });
  console.log(
    `[day-base-pickup] scheduled category=${params.category} runYmd=${params.runYmd} attempt=${params.attempt} delaySeconds=${DAY_BASE_PICKUP_DELAY_SECONDS}`,
  );
};

export const consumeDayBasePickup = async (params: ConsumeDayBasePickupParams): Promise<void> => {
  const { env, message } = params;
  const { category, runYmd, attempt } = message;
  if ((await headDayBaseObject({ category, env, runYmd })) !== null) {
    console.log(
      `[day-base-pickup] already-landed category=${category} runYmd=${runYmd} attempt=${attempt}`,
    );
    return;
  }
  const picked = await pickUpPrewarmDayBase({ category, env, runYmd });
  if (picked && (await headDayBaseObject({ category, env, runYmd })) !== null) {
    console.log(
      `[day-base-pickup] landed category=${category} runYmd=${runYmd} attempt=${attempt}`,
    );
    return;
  }
  if (attempt >= DAY_BASE_PICKUP_MAX_ATTEMPTS) {
    console.warn(
      `[day-base-pickup] exhausted category=${category} runYmd=${runYmd} attempt=${attempt}`,
    );
    return;
  }
  await enqueueDayBasePickup({
    attempt: attempt + 1,
    category,
    env,
    runYmd,
  });
};
