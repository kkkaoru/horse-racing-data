// Run with bun. Delayed FEATURES_CACHE pickup after a detached day-base prewarm.
//
// /prewarm-day-base returns accepted as soon as DAY_CHAIN starts. An immediate
// GET /prewarm-day-base-cache then reads an empty in-process store, so R2 stays
// missing and every later race logs r2-missing-object. This module re-enqueues
// pickup after the first-day build (10-15m) has had time to commit.

import { pickUpPrewarmDayBaseWithOutcome } from "./day-base-prewarm-pickup";
import { CONTAINER_DAY_BASE_SLOT_STALE_MS, type ContainerSlotKind } from "./container-slot-cap";
import { handOffContainerStopOrCleanup } from "./container-cleanup";
import { claimContainerSlot, releaseContainerSlot } from "./do-state";
import { fanOutPredictionsAfterDayBaseHit } from "./feature-hit-prediction";
import { materializeDayBasePerRaceCache } from "./day-base-race-materializer";
import { getFocusedFullDayBaseReadiness } from "./focused-full-day-base-readiness";
import { kickRunningStylePlan } from "./running-style-kick";
import { isOldDateRunYmd } from "./old-date-guard";
import { PREDICT_DO_NAME_PREFIX } from "./predict-do-shard";
import { claimDayBaseGeneration } from "./predict-run-coordinator";
import type { DayBasePickupMessage, Env, PredictCategory } from "./types";

interface EnqueueDayBasePickupParams {
  attempt: number;
  category: PredictCategory;
  env: Env;
  generationId?: string;
  runYmd: string;
  generatePredictionsAfterHit?: boolean;
  force?: boolean;
}

interface CleanupDayBaseWorkParams {
  category: PredictCategory;
  env: Env;
  generationId?: string;
  runYmd: string;
}

interface ConsumeDayBasePickupParams {
  env: Env;
  message: DayBasePickupMessage;
}

interface CompleteLandedDayBaseParams extends CleanupDayBaseWorkParams {
  generatePredictionsAfterHit: boolean;
}

interface ExhaustDayBasePickupParams extends CleanupDayBaseWorkParams {
  attempt: number;
}

interface RestartStaleDayBaseParams {
  category: PredictCategory;
  env: Env;
  generationId?: string;
  runYmd: string;
}

interface DayBaseWorkKeyParams {
  category: PredictCategory;
  generationId?: string;
  runYmd: string;
}

type StaleRestartOutcome = "accepted" | "busy" | "completed" | "failed";

export const DAY_BASE_PICKUP_TYPE = "day-base-pickup";
export const DAY_BASE_PICKUP_DELAY_SECONDS = 180;
// The Python day-base pipeline has a 30-minute deadline. Twelve 3-minute
// pickup polls cover 36 minutes, leaving a bounded margin for the final
// upload/readiness check and owner-safe stop handoff. Eight polls (24 minutes)
// could stop a still-running detached build before its 30-minute deadline,
// which discarded the only complete day-base and forced every race into the
// expensive full fallback. The budget remains finite so stale Queue messages
// cannot resurrect a container indefinitely.
export const DAY_BASE_PICKUP_MAX_ATTEMPTS = 12;
export const DAY_BASE_PICKUP_FIRST_ATTEMPT = 1;
const PREWARM_DAY_BASE_PATH: string = "/prewarm-day-base";
const PREDICT_HOST: string = "http://do";
const STALE_REBUILD_DAYS_AHEAD: number = 0;
const STALE_REBUILD_ACCEPTED_PATTERN: RegExp = /"status"\s*:\s*"accepted"/u;
const STALE_REBUILD_COMPLETED_PATTERN: RegExp = /"status"\s*:\s*"success"/u;
const STALE_ROW_COUNT_REASON_PATTERN: RegExp = /^(?:rs|source)-row-count-\d+-of-\d+$/u;
const STALE_EXACT_REASONS: ReadonlySet<string> = new Set([
  "rs-predicted-at-max-mismatch",
  "source-watermark-mismatch",
]);
const DAY_BASE_SLOT_KIND: ContainerSlotKind = "day-base";
const GENERATION_ID_PATTERN: RegExp = /^[A-Za-z0-9_-]{1,128}$/u;

export const dayBaseGenerationFields = (
  generationId: string | undefined,
): { generationId?: string } => (generationId === undefined ? {} : { generationId });

export const buildDayBaseWorkKey = (params: DayBaseWorkKeyParams): string =>
  `day-base:${params.runYmd}:${params.category}${params.generationId === undefined ? "" : `:${params.generationId}`}`;

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
  if (
    value.generationId !== undefined &&
    (typeof value.generationId !== "string" || !GENERATION_ID_PATTERN.test(value.generationId))
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
    ...dayBaseGenerationFields(params.generationId),
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

const isKnownStaleReadinessReason = (reason: string): boolean =>
  STALE_ROW_COUNT_REASON_PATTERN.test(reason) || STALE_EXACT_REASONS.has(reason);

const buildStaleDayBaseWorkKey = (params: DayBaseWorkKeyParams): string =>
  `day-base-stale:${params.runYmd}:${params.category}${params.generationId === undefined ? "" : `:${params.generationId}`}`;

export const cleanupDayBaseWork = (params: CleanupDayBaseWorkParams): Promise<void> => {
  const canonicalWorkKey = buildDayBaseWorkKey(params);
  const staleWorkKey = buildStaleDayBaseWorkKey(params);
  return handOffContainerStopOrCleanup({
    acceptableWorkKeys: [canonicalWorkKey, staleWorkKey],
    env: params.env,
    name: `${PREDICT_DO_NAME_PREFIX}${params.category}`,
    role: "legacy",
    workKey: canonicalWorkKey,
  });
};

const releaseStaleDayBaseSlot = (params: RestartStaleDayBaseParams): Promise<void> =>
  releaseContainerSlot({
    doName: `${PREDICT_DO_NAME_PREFIX}${params.category}`,
    env: params.env,
    kind: DAY_BASE_SLOT_KIND,
    workKey: buildStaleDayBaseWorkKey(params),
  });

const exhaustDayBasePickup = async (params: ExhaustDayBasePickupParams): Promise<void> => {
  console.warn(
    `[day-base-pickup] exhausted category=${params.category} runYmd=${params.runYmd} attempt=${params.attempt}`,
  );
  // Preserve whichever day-base owner is active until the single stop
  // consumer atomically matches, destroys, and clears it.
  await cleanupDayBaseWork(params);
};

export const completeLandedDayBase = async (
  params: CompleteLandedDayBaseParams,
): Promise<number> => {
  const materialized = await materializeDayBasePerRaceCache({
    category: params.category,
    env: params.env,
    runYmd: params.runYmd,
  });
  if (materialized.status !== "materialized") {
    throw new Error(
      `per-race foundation warm failed category=${params.category} runYmd=${params.runYmd} reason=${materialized.reason}`,
    );
  }
  const racesEnqueued = await (
    params.generatePredictionsAfterHit
      ? fanOutPredictionsAfterDayBaseHit({
          category: params.category,
          env: params.env,
          runYmd: params.runYmd,
        })
      : Promise.resolve(0)
  ).catch(async (completionError: unknown): Promise<never> => {
    try {
      await cleanupDayBaseWork(params);
    } catch (cleanupError) {
      throw new AggregateError(
        [completionError, cleanupError],
        `Day-base completion and Container cleanup failed category=${params.category} runYmd=${params.runYmd}`,
      );
    }
    throw completionError;
  });
  // Keep the coordinator lease until the stop consumer destroys the exact
  // owner and clears its slot. Releasing first creates a race where a newer
  // same-day claim can appear before the delayed stop is checked.
  await cleanupDayBaseWork(params);
  return racesEnqueued;
};

const fetchStaleDayBaseRestart = async (
  params: RestartStaleDayBaseParams,
): Promise<Exclude<StaleRestartOutcome, "busy">> => {
  const doName = `${PREDICT_DO_NAME_PREFIX}${params.category}`;
  const doId = params.env.FINISH_POSITION_PREDICT_CONTAINER.idFromName(doName);
  const stub = params.env.FINISH_POSITION_PREDICT_CONTAINER.get(doId);
  const searchParams = new URLSearchParams({
    category: params.category,
    daysAhead: String(STALE_REBUILD_DAYS_AHEAD),
    runDate: params.runYmd,
  });
  const response = await stub.fetch(
    new Request(`${PREDICT_HOST}${PREWARM_DAY_BASE_PATH}?${searchParams.toString()}`),
  );
  if (!response.ok) {
    console.error(
      `[day-base-pickup] stale rebuild failed category=${params.category} runYmd=${params.runYmd} status=${response.status}`,
    );
    return "failed";
  }
  const responseText = await response.text();
  if (STALE_REBUILD_ACCEPTED_PATTERN.test(responseText)) return "accepted";
  if (STALE_REBUILD_COMPLETED_PATTERN.test(responseText)) return "completed";
  console.error(
    `[day-base-pickup] stale rebuild rejected category=${params.category} runYmd=${params.runYmd}`,
  );
  return "failed";
};

const restartStaleDayBase = async (
  params: RestartStaleDayBaseParams,
): Promise<StaleRestartOutcome> => {
  const doName = `${PREDICT_DO_NAME_PREFIX}${params.category}`;
  const canonicalWorkKey = buildDayBaseWorkKey(params);
  const staleWorkKey = buildStaleDayBaseWorkKey(params);
  const claim = await claimContainerSlot({
    category: params.category,
    doName,
    env: params.env,
    kind: DAY_BASE_SLOT_KIND,
    replaceWorkKey: canonicalWorkKey,
    staleAfterMs: CONTAINER_DAY_BASE_SLOT_STALE_MS,
    workKey: staleWorkKey,
  });
  if (!claim.proceed) {
    console.warn(
      `[day-base-pickup] stale rebuild slot ${claim.state ?? "busy"} category=${params.category} runYmd=${params.runYmd}`,
    );
    return "busy";
  }
  try {
    const outcome = await fetchStaleDayBaseRestart(params);
    if (outcome === "accepted") {
      console.warn(
        `[day-base-pickup] stale candidate rebuild started category=${params.category} runYmd=${params.runYmd}`,
      );
      return outcome;
    }
    await releaseStaleDayBaseSlot(params);
    if (outcome === "completed") {
      console.warn(
        `[day-base-pickup] stale candidate rebuild completed category=${params.category} runYmd=${params.runYmd}`,
      );
    }
    return outcome;
  } catch (error) {
    try {
      await releaseStaleDayBaseSlot(params);
    } catch (releaseError) {
      console.error(
        `[day-base-pickup] stale rebuild slot release failed category=${params.category} runYmd=${params.runYmd}: ${String(releaseError)}`,
      );
    }
    throw error;
  }
};

export const consumeDayBasePickup = async (params: ConsumeDayBasePickupParams): Promise<void> => {
  const { env, message } = params;
  const { category, runYmd, attempt, generationId } = message;
  // Queue pickup is automatic lifecycle work, not a historical regeneration
  // command. After the JST date rolls over, a delayed delivery must only hand
  // off owner-safe stops for either lease identity. Probing readiness or the
  // Container first can cold-start a standard-4 and restart the stale build.
  // Historical regeneration belongs to the local backfill path. `force`
  // bypasses freshness and deduplication only; it must never bypass the date
  // fence because automatic repair messages also carry force=true.
  if (isOldDateRunYmd(runYmd, new Date())) {
    console.warn(
      `[day-base-pickup] dropping past automatic pickup category=${category} runYmd=${runYmd} attempt=${attempt}`,
    );
    await cleanupDayBaseWork({
      category,
      env,
      ...dayBaseGenerationFields(generationId),
      runYmd,
    });
    return;
  }
  const generation = await claimDayBaseGeneration({
    category,
    env,
    ...dayBaseGenerationFields(generationId),
    phase: "pickup",
    runYmd,
  });
  if (generation.state === "superseded") {
    console.warn(
      `[day-base-pickup] dropping superseded generation category=${category} runYmd=${runYmd} attempt=${attempt}`,
    );
    await cleanupDayBaseWork({
      category,
      env,
      ...dayBaseGenerationFields(generationId),
      runYmd,
    });
    return;
  }
  if (generation.state === "preempting") {
    console.warn(
      `[day-base-pickup] preempting later generation category=${category} runYmd=${runYmd} attempt=${attempt} preemptedWorkKey=${generation.preemptedWorkKey}`,
    );
    await handOffContainerStopOrCleanup({
      env,
      name: `${PREDICT_DO_NAME_PREFIX}${category}`,
      role: "legacy",
      workKey: generation.preemptedWorkKey,
    });
    if (attempt >= DAY_BASE_PICKUP_MAX_ATTEMPTS) {
      await exhaustDayBasePickup({
        attempt,
        category,
        env,
        ...dayBaseGenerationFields(generationId),
        runYmd,
      });
      return;
    }
    await enqueueDayBasePickup({
      attempt: attempt + 1,
      category,
      env,
      ...dayBaseGenerationFields(generationId),
      ...(message.generatePredictionsAfterHit === true
        ? { generatePredictionsAfterHit: true }
        : {}),
      ...(message.force === true ? { force: true } : {}),
      runYmd,
    });
    return;
  }
  // A delayed pickup may arrive after another pickup already committed the
  // same live generation and stopped its Container.  Fetching the DO first in
  // that case cold-starts a standard-4 solely to discover that its in-process
  // cache is empty.  Canonical R2 readiness is sufficient idempotency proof:
  // it compares the object metadata with the current Catalog/RS watermarks,
  // so a genuinely newer generation still fails this fast path and continues
  // to the Container pickup below.
  const existingReadiness =
    message.force === true
      ? { ready: false, reason: "force-generation-pending" }
      : await getFocusedFullDayBaseReadiness({ category, env, runYmd }).catch(() => ({
          ready: false,
          reason: "readiness-error",
        }));
  if (existingReadiness.ready) {
    console.log(
      `[day-base-pickup] already-landed category=${category} runYmd=${runYmd} attempt=${attempt}`,
    );
    await completeLandedDayBase({
      category,
      env,
      ...dayBaseGenerationFields(generationId),
      generatePredictionsAfterHit: message.generatePredictionsAfterHit === true,
      runYmd,
    });
    return;
  }
  // An old R2 object can still be present while the detached build is
  // producing a new watermark. Only a successful container pickup may prove
  // this generation landed; presence alone must not acknowledge the message.
  const pickupOutcome = await pickUpPrewarmDayBaseWithOutcome({ category, env, runYmd });
  if (pickupOutcome === "transient-error") {
    throw new Error(
      `Transient day-base pickup failure category=${category} runYmd=${runYmd} attempt=${attempt}`,
    );
  }
  if (pickupOutcome === "foundation-landed") {
    console.log(
      `[day-base-pickup] foundation-landed category=${category} runYmd=${runYmd} attempt=${attempt}`,
    );
    // A future-day foundation can land before sync-realtime-data's normal
    // today/tomorrow running-style cron window. Kick its idempotent planner
    // on every pickup retry so the final readiness cannot wait for rollover.
    if (env.RUNNING_STYLE_PLAN_JOBS) {
      await kickRunningStylePlan({ date: runYmd, env }).catch((error: unknown) => {
        console.warn(
          `[day-base-pickup] running-style planner kick failed category=${category} runYmd=${runYmd}: ${String(error)}`,
        );
      });
    }
    if (attempt >= DAY_BASE_PICKUP_MAX_ATTEMPTS) {
      await exhaustDayBasePickup({
        attempt,
        category,
        env,
        ...dayBaseGenerationFields(generationId),
        runYmd,
      });
      return;
    }
    await enqueueDayBasePickup({
      attempt: attempt + 1,
      category,
      env,
      ...dayBaseGenerationFields(generationId),
      ...(message.generatePredictionsAfterHit === true
        ? { generatePredictionsAfterHit: true }
        : {}),
      ...(message.force === true ? { force: true } : {}),
      runYmd,
    });
    return;
  }
  const picked = pickupOutcome === "landed";
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
    await completeLandedDayBase({
      category,
      env,
      ...dayBaseGenerationFields(generationId),
      generatePredictionsAfterHit: message.generatePredictionsAfterHit === true,
      runYmd,
    });
    return;
  }
  if (picked) {
    console.warn(
      `[day-base-pickup] rejected stale pickup category=${category} runYmd=${runYmd} attempt=${attempt} reason=${readiness.reason}`,
    );
  }
  const knownStaleReadiness = isKnownStaleReadinessReason(existingReadiness.reason);
  const pickedStaleReadiness = picked && isKnownStaleReadinessReason(readiness.reason);
  if (pickupOutcome === "stale" || knownStaleReadiness || pickedStaleReadiness) {
    const restartOutcome = await restartStaleDayBase({
      category,
      env,
      ...dayBaseGenerationFields(generationId),
      runYmd,
    }).catch((error) => {
      console.error(
        `[day-base-pickup] stale rebuild request failed category=${category} runYmd=${runYmd}: ${String(error)}`,
      );
      return "failed" satisfies StaleRestartOutcome;
    });
    if (restartOutcome === "accepted" || restartOutcome === "completed") {
      await enqueueDayBasePickup({
        attempt: DAY_BASE_PICKUP_FIRST_ATTEMPT,
        category,
        env,
        ...dayBaseGenerationFields(generationId),
        ...(message.generatePredictionsAfterHit === true
          ? { generatePredictionsAfterHit: true }
          : {}),
        ...(message.force === true ? { force: true } : {}),
        runYmd,
      });
      return;
    }
  }
  if (
    attempt >= DAY_BASE_PICKUP_MAX_ATTEMPTS &&
    (existingReadiness.reason === "readiness-error" || readiness.reason === "readiness-error")
  ) {
    throw new Error(
      `Transient day-base readiness failure category=${category} runYmd=${runYmd} attempt=${attempt}`,
    );
  }
  if (attempt >= DAY_BASE_PICKUP_MAX_ATTEMPTS) {
    await exhaustDayBasePickup({
      attempt,
      category,
      env,
      ...dayBaseGenerationFields(generationId),
      runYmd,
    });
    return;
  }
  await enqueueDayBasePickup({
    attempt: attempt + 1,
    category,
    env,
    ...dayBaseGenerationFields(generationId),
    ...(message.generatePredictionsAfterHit === true ? { generatePredictionsAfterHit: true } : {}),
    ...(message.force === true ? { force: true } : {}),
    runYmd,
  });
};
