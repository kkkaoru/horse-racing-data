// Run with bun. Queue consumer: processes one predict message per batch invocation.
// For each message: dedup via DO coordinator (strong consistency), call the Container
// DO stub's fetch, track state.

import { claimFocusedFullRace, claimRun, completeFocusedFullRace, completeRun } from "./do-state";
import {
  parseNdjsonStream,
  type PredictProgressLine,
  type PredictResultLine,
} from "./ndjson-stream";
import {
  warmPredictionCacheForCategory,
  warmPredictionCacheForRace,
} from "./prediction-cache-warm";
import { isFocusedFullPredictionComplete } from "./focused-full-completion";
import type { Env, PredictQueueMessage } from "./types";

const RUN_YMD_YEAR_START = 0;
const RUN_YMD_YEAR_END = 4;
const RUN_YMD_MONTH_START = 4;
const RUN_YMD_MONTH_END = 6;
const RUN_YMD_DAY_START = 6;
const RUN_YMD_DAY_END = 8;
const PREDICT_DO_NAME_PREFIX = "predict-";
const PREDICT_PATH = "/predict";
const PREDICT_HOST = "http://do";
const RESCORE_MODE = "rescore";
const RESULT_SUCCESS_STATUS = "success";
// Focused full returns "accepted" when the container either launched a detached
// pipeline for this race or observed the same race already in flight. The queue
// consumer polls Neon completion on delayed redeliveries instead of holding a
// Cloudflare request open for the whole feature chain.
const FOCUSED_FULL_ACCEPTED_STATUS = "accepted";
const FOCUSED_FULL_BUSY_STATUS = "busy";
const FOCUSED_FULL_ALREADY_COMPLETE_STATUS = "already-complete";
// The container returns "busy" when another race in the same category holds its
// single per-process pipeline slot. Re-enqueue a fresh copy (which resets the
// message's retry attempt count) so the starved race keeps waiting for the
// slot WITHOUT burning the DLQ retry budget, bounded by MAX_BUSY_REQUEUES so a
// permanently-stuck slot still surfaces via the DLQ. The re-enqueue delay
// grows with busyRequeueCount (see computeBusyRequeueDelaySeconds below)
// rather than staying fixed: each redelivery re-runs the Neon focused-full
// completion guard, so a short delay while the slot is likely to free up soon
// and a longer delay once many races are already queued ahead keeps
// slot-pickup latency low without scaling Neon query volume linearly with
// how deep in a same-day burst this message sits.
const BUSY_REQUEUE_DELAY_BASE_SECONDS = 30;
const BUSY_REQUEUE_DELAY_STEP_SECONDS = 20;
const BUSY_REQUEUE_DELAY_MAX_SECONDS = 300;
// A single per-category container slot serializes every race queued for that
// category on a given day. Sized so the delay schedule above (30s, 50s, ...,
// capped at 300s) sums to roughly 3 hours of patience for the last message in
// the queue, comfortably outlasting a worst-case same-day single-category
// burst of up to ~80 races plus one slow cold-start predecessor.
const MAX_BUSY_REQUEUES = 45;
const BUSY_REQUEUE_COUNT_ZERO = 0;
const BUSY_REQUEUE_COUNT_INCREMENT = 1;
// Retry budget reasoning: the Python container's focused-full pipeline
// (DuckDB base build + 10-17 sequential layer scripts +
// CatBoost/XGBoost scoring + Neon UPSERT) has been observed taking 10-20+
// minutes end-to-end (NAR ~13-17 min extrapolated from partial timing; JRA has
// ~1.6x as many layers so plausibly up to ~25-27 min).
// FOCUSED_FULL_RETRY_DELAY_SECONDS (2.5 min) x max_retries (12, set in
// wrangler.jsonc) gives a 30-minute total retry budget per message --
// comfortably above the worst-case observed/extrapolated single-run duration,
// while each individual delivery after launch is a cheap fast completion /
// accepted check (not a re-run) once the in-container single-slot guard sees
// that race already in flight for that category.
const FOCUSED_FULL_RETRY_DELAY_SECONDS = 150;
const FOCUSED_FULL_IN_FLIGHT_STALE_MS = 35 * 60 * 1000;
const JRA_CATEGORY = "jra";
const NAR_CATEGORY = "nar";
const BAN_EI_CATEGORY = "ban-ei";
// Categories whose per-race rescore is served by the container DO held /predict
// (Python ensemble re-score). Keeping JRA here avoids a stale Worker-native
// scorer path drifting away from the container's production model contract.
const CONTAINER_PER_RACE_CATEGORIES = new Set<string>([
  JRA_CATEGORY,
  NAR_CATEGORY,
  BAN_EI_CATEGORY,
]);

interface PredictUrlParams {
  category: string;
  daysAhead: number;
  debug?: boolean;
  keibajoCode?: string;
  mode: string;
  raceBango?: string;
  // runYmd is the YYYYMMDD date string required by the container /predict endpoint.
  runYmd: string;
}

// PredictQueueMessage with the per-race target fields proven present. isPerRaceRescore
// narrows to this so the container path never needs an unreachable undefined guard.
interface PerRaceRescoreMessage extends PredictQueueMessage {
  keibajoCode: string;
  raceBango: string;
}

interface FocusedFullSkipDedupMessage extends PredictQueueMessage {
  keibajoCode: string;
  raceBango: string;
  mode: "full";
  skipDedup: true;
}

interface PerRaceRescoreUrlParams {
  category: string;
  daysAhead: number;
  debug?: boolean;
  // keibajoCode / raceBango are 2-digit zero-padded strings from the per-race coordinator.
  keibajoCode: string;
  raceBango: string;
  // runYmd is the YYYYMMDD date string required by the container /predict endpoint.
  runYmd: string;
}

interface PredictDoNameParams {
  category: string;
  keibajoCode?: string;
  raceBango?: string;
  runYmd: string;
}

const buildPredictDoName = ({ category }: PredictDoNameParams): string => {
  // Per-race messages still carry race scope in the /predict query. The
  // Container instance is category-scoped to avoid exhausting max_instances while
  // previous race-scoped instances wait through sleepAfter.
  return `${PREDICT_DO_NAME_PREFIX}${category}`;
};

const buildPredictUrl = (params: PredictUrlParams): string => {
  const searchParams = new URLSearchParams({
    category: params.category,
    daysAhead: String(params.daysAhead),
    mode: params.mode,
    runDate: params.runYmd,
  });
  if (params.keibajoCode) searchParams.set("keibajoCode", params.keibajoCode);
  if (params.raceBango) searchParams.set("raceBango", params.raceBango);
  if (params.debug === true) searchParams.set("debug", "1");
  return `${PREDICT_HOST}${PREDICT_PATH}?${searchParams.toString()}`;
};

const buildPerRaceRescoreUrl = (params: PerRaceRescoreUrlParams): string => {
  const searchParams = new URLSearchParams({
    category: params.category,
    daysAhead: String(params.daysAhead),
    mode: RESCORE_MODE,
    keibajoCode: params.keibajoCode,
    raceBango: params.raceBango,
    runDate: params.runYmd,
  });
  if (params.debug === true) searchParams.set("debug", "1");
  return `${PREDICT_HOST}${PREDICT_PATH}?${searchParams.toString()}`;
};

const debugLog = (body: Pick<PredictQueueMessage, "debug">, message: string): void => {
  if (body.debug === true) console.log(message);
};

// A per-race rescore is targeted at one race (mode="rescore" with both a
// keibajo_code and a race_bango set by the per-race coordinator). Per-category
// rescores (no keibajo_code) stay on the container path. Narrows the message so the
// keibajoCode / raceBango are known-present downstream (no unreachable guard needed).
const isPerRaceRescore = (
  message: Message<PredictQueueMessage>,
): message is Message<PerRaceRescoreMessage> =>
  message.body.mode === RESCORE_MODE &&
  message.body.keibajoCode !== undefined &&
  message.body.raceBango !== undefined;

const assertPredictResultSucceeded = (result: PredictResultLine): void => {
  if (result.status === undefined || result.status === RESULT_SUCCESS_STATUS) return;
  const detail = result.error ? `: ${result.error}` : "";
  throw new Error(`Container result status=${result.status}${detail}`);
};

const isFocusedSkipDedupMessage = (
  message: PredictQueueMessage,
): message is FocusedFullSkipDedupMessage =>
  message.skipDedup === true &&
  message.mode === "full" &&
  message.keibajoCode !== undefined &&
  message.raceBango !== undefined;

const warmPredictionCacheForFocusedRace = (message: FocusedFullSkipDedupMessage): void => {
  void warmPredictionCacheForRace({
    day: message.runYmd.slice(RUN_YMD_DAY_START, RUN_YMD_DAY_END),
    keibajoCode: message.keibajoCode,
    month: message.runYmd.slice(RUN_YMD_MONTH_START, RUN_YMD_MONTH_END),
    raceNumber: message.raceBango,
    year: message.runYmd.slice(RUN_YMD_YEAR_START, RUN_YMD_YEAR_END),
  });
};

const ackIfFocusedFullAlreadyComplete = async (
  message: Message<PredictQueueMessage>,
  env: Env,
): Promise<boolean> => {
  if (!isFocusedSkipDedupMessage(message.body)) return false;
  const { category, keibajoCode, raceBango, runYmd } = message.body;
  try {
    debugLog(
      message.body,
      `[predict-queue] focused-completion-check start ${describePredictMessage(message.body)}`,
    );
    const complete = await isFocusedFullPredictionComplete({
      category,
      env,
      keibajoCode,
      raceBango,
      runYmd,
    });
    debugLog(
      message.body,
      `[predict-queue] focused-completion-check result ${describePredictMessage(
        message.body,
      )} complete=${complete}`,
    );
    if (!complete) return false;
    await completeFocusedFullRace({
      category,
      env,
      keibajoCode,
      raceBango,
      runYmd,
      status: "success",
    });
    debugLog(
      message.body,
      `Skipping focused full already complete category=${category} runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango}`,
    );
    message.ack();
    warmPredictionCacheForFocusedRace(message.body);
    return true;
  } catch (err) {
    console.warn(
      `Focused full completion guard failed category=${category} runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango}:`,
      String(err),
    );
    return false;
  }
};

const retryFocusedFullAlreadyInFlight = (message: Message<PredictQueueMessage>): void => {
  const { category, keibajoCode, raceBango, runYmd } = message.body;
  debugLog(
    message.body,
    `Focused full already in flight category=${category} runYmd=${runYmd}${raceScopeSuffix(
      keibajoCode,
      raceBango,
    )} -- will re-check on redelivery`,
  );
  message.retry({ delaySeconds: FOCUSED_FULL_RETRY_DELAY_SECONDS });
};

const claimFocusedFullOrRetry = async (
  message: Message<PredictQueueMessage>,
  body: FocusedFullSkipDedupMessage,
  env: Env,
): Promise<boolean> => {
  const { category, keibajoCode, raceBango, runYmd } = body;
  const claim = await claimFocusedFullRace({
    category,
    env,
    keibajoCode,
    raceBango,
    runYmd,
    staleAfterMs: FOCUSED_FULL_IN_FLIGHT_STALE_MS,
  });
  debugLog(
    body,
    `[predict-queue] focused-claim ${describePredictMessage(body)} proceed=${
      claim.proceed
    } state=${claim.state ?? "-"} staleAfterMs=${FOCUSED_FULL_IN_FLIGHT_STALE_MS}`,
  );
  if (claim.proceed) return true;
  retryFocusedFullAlreadyInFlight(message);
  return false;
};

const raceScopeSuffix = (keibajoCode?: string, raceBango?: string): string => {
  let suffix = "";
  if (keibajoCode !== undefined) suffix += ` keibajo=${keibajoCode}`;
  if (raceBango !== undefined) suffix += ` race=${raceBango}`;
  return suffix;
};

const describePredictMessage = (body: PredictQueueMessage): string =>
  [
    `category=${body.category}`,
    `runYmd=${body.runYmd}`,
    `mode=${body.mode}`,
    `daysAhead=${body.daysAhead}`,
    `skipDedup=${body.skipDedup === true}`,
    `busyRequeueCount=${body.busyRequeueCount ?? BUSY_REQUEUE_COUNT_ZERO}`,
  ].join(" ") + raceScopeSuffix(body.keibajoCode, body.raceBango);

// Grows the busy re-enqueue delay with how many times this message has already
// been requeued busy, capped at BUSY_REQUEUE_DELAY_MAX_SECONDS.
const computeBusyRequeueDelaySeconds = (busyRequeueCount: number): number =>
  Math.min(
    BUSY_REQUEUE_DELAY_BASE_SECONDS + BUSY_REQUEUE_DELAY_STEP_SECONDS * busyRequeueCount,
    BUSY_REQUEUE_DELAY_MAX_SECONDS,
  );

// This focused-full message's race never started: the container's single
// per-process pipeline slot for this category was busy with a DIFFERENT race.
const requeueBusyFocusedFull = async (
  message: Message<PredictQueueMessage>,
  env: Env,
): Promise<void> => {
  const { busyRequeueCount, category, keibajoCode, raceBango, runYmd } = message.body;
  const currentCount = busyRequeueCount ?? BUSY_REQUEUE_COUNT_ZERO;
  const suffix = raceScopeSuffix(keibajoCode, raceBango);
  if (currentCount >= MAX_BUSY_REQUEUES) {
    console.warn(
      `Focused full slot busy budget exhausted category=${category} runYmd=${runYmd}${suffix} busyRequeueCount=${currentCount} -- retrying toward DLQ`,
    );
    message.retry();
    return;
  }
  const nextCount = currentCount + BUSY_REQUEUE_COUNT_INCREMENT;
  if (isFocusedSkipDedupMessage(message.body)) {
    await completeFocusedFullRace({
      category,
      env,
      keibajoCode: message.body.keibajoCode,
      raceBango: message.body.raceBango,
      runYmd,
      status: "error",
    });
  }
  const delaySeconds = computeBusyRequeueDelaySeconds(currentCount);
  await env.PREDICT_QUEUE.send({ ...message.body, busyRequeueCount: nextCount }, { delaySeconds });
  debugLog(
    message.body,
    `Focused full slot busy, re-enqueued category=${category} runYmd=${runYmd}${suffix} busyRequeueCount=${nextCount} delaySeconds=${delaySeconds}`,
  );
  message.ack();
};

// Dispatches focused per-race full statuses ("accepted" / "already-complete" /
// "busy") to their handling and returns whether the status was one of these
// (in which case processMessage's caller must stop, having already
// acked/retried). Returns false for any other status (e.g. "success" or
// "error") so the caller falls through to the shared success/error handling.
const handleFocusedFullStatus = async (
  message: Message<PredictQueueMessage>,
  env: Env,
  status: string | undefined,
): Promise<boolean> => {
  const { category, keibajoCode, raceBango, runYmd } = message.body;
  const suffix = raceScopeSuffix(keibajoCode, raceBango);
  if (status === FOCUSED_FULL_ACCEPTED_STATUS) {
    debugLog(
      message.body,
      `Focused full accepted, still in progress category=${category} runYmd=${runYmd}${suffix} -- will re-check on redelivery`,
    );
    message.retry({ delaySeconds: FOCUSED_FULL_RETRY_DELAY_SECONDS });
    return true;
  }
  if (status === FOCUSED_FULL_ALREADY_COMPLETE_STATUS) {
    debugLog(
      message.body,
      `Focused full already complete (container) category=${category} runYmd=${runYmd}${suffix}`,
    );
    if (isFocusedSkipDedupMessage(message.body)) {
      await completeFocusedFullRace({
        category,
        env,
        keibajoCode: message.body.keibajoCode,
        raceBango: message.body.raceBango,
        runYmd,
        status: "success",
      });
    }
    message.ack();
    if (isFocusedSkipDedupMessage(message.body)) {
      warmPredictionCacheForFocusedRace(message.body);
    }
    return true;
  }
  if (status === FOCUSED_FULL_BUSY_STATUS) {
    await requeueBusyFocusedFull(message, env);
    return true;
  }
  return false;
};

const logPredictProgress = (message: PredictQueueMessage, line: PredictProgressLine): void => {
  if (message.debug !== true) return;
  console.log(
    `Predict progress category=${message.category} runYmd=${message.runYmd} keibajo=${
      message.keibajoCode ?? "-"
    } race=${message.raceBango ?? "-"} stage=${line.stage ?? line.message ?? "-"} elapsed=${
      line.elapsed_s ?? line.elapsed ?? "-"
    }`,
  );
};

// Container per-race rescore: held /predict on a per-race DO
// runs the Python ensemble re-score for one race. No per-category run state is
// touched (completeRun is not called) so concurrent full/per-category runs are
// unaffected. A successful NDJSON result (status omitted or success) acks whether
// racesPredicted is > 0 or 0 (cache_miss). Fetch / stream / DO errors, and final
// result status:error, retry via the queue's DLQ machinery. After a
// successful ack the viewer Cache API is warmed for the same race so the
// event-driven horse-weight trigger surfaces fresh predictions on the race
// detail page without waiting for cache TTL. Warm is fire-and-forget: failures
// are swallowed inside the warm helper.
const processContainerPerRaceRescore = async (
  message: Message<PerRaceRescoreMessage>,
  env: Env,
): Promise<void> => {
  const { category, daysAhead, debug, keibajoCode, raceBango, runYmd } = message.body;
  const startedAt = Date.now();
  const predictDoName = buildPredictDoName({ category, keibajoCode, raceBango, runYmd });
  const predictUrl = buildPerRaceRescoreUrl({
    category,
    daysAhead,
    debug,
    keibajoCode,
    raceBango,
    runYmd,
  });
  const doId = env.FINISH_POSITION_PREDICT_CONTAINER.idFromName(predictDoName);
  const stub = env.FINISH_POSITION_PREDICT_CONTAINER.get(doId);
  try {
    debugLog(
      message.body,
      `[predict-queue] container-fetch start ${describePredictMessage(
        message.body,
      )} doName=${predictDoName} url=${predictUrl}`,
    );
    const response = await stub.fetch(new Request(predictUrl));
    debugLog(
      message.body,
      `[predict-queue] container-fetch response ${describePredictMessage(
        message.body,
      )} doName=${predictDoName} status=${response.status} ok=${response.ok} durationMs=${
        Date.now() - startedAt
      }`,
    );
    if (!response.body) throw new Error("Empty response from predict DO");
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Container DO returned ${response.status}: ${text}`);
    }
    const result = await parseNdjsonStream(response.body, {
      onProgress(line) {
        logPredictProgress(message.body, line);
      },
    });
    assertPredictResultSucceeded(result);
    console.log(
      `Rescore container category=${category} runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango} races=${result.racesPredicted} durationMs=${
        Date.now() - startedAt
      }`,
    );
    message.ack();
    void warmPredictionCacheForRace({
      day: runYmd.slice(RUN_YMD_DAY_START, RUN_YMD_DAY_END),
      keibajoCode,
      month: runYmd.slice(RUN_YMD_MONTH_START, RUN_YMD_MONTH_END),
      raceNumber: raceBango,
      year: runYmd.slice(RUN_YMD_YEAR_START, RUN_YMD_YEAR_END),
    });
  } catch (err) {
    console.error(
      `Container per-race rescore failed category=${category} runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango} durationMs=${
        Date.now() - startedAt
      }:`,
      String(err),
    );
    message.retry();
  }
};

// Per-race rescore dispatch: supported categories route to the container held
// /predict, unknown categories are skipped + acked.
const processPerRaceRescore = (
  message: Message<PerRaceRescoreMessage>,
  env: Env,
): Promise<void> => {
  const { category, runYmd } = message.body;
  if (CONTAINER_PER_RACE_CATEGORIES.has(category))
    return processContainerPerRaceRescore(message, env);
  console.warn(
    `Skipping per-race rescore for unsupported category=${category} runYmd=${runYmd}${raceScopeSuffix(
      message.body.keibajoCode,
      message.body.raceBango,
    )}`,
  );
  message.ack();
  return Promise.resolve();
};

const processMessage = async (message: Message<PredictQueueMessage>, env: Env): Promise<void> => {
  debugLog(message.body, `[predict-queue] received ${describePredictMessage(message.body)}`);
  if (isPerRaceRescore(message)) return processPerRaceRescore(message, env);
  const { category, runYmd, daysAhead, mode, keibajoCode, raceBango, skipDedup } = message.body;
  const startedAt = Date.now();
  const isFocusedSkipDedup = isFocusedSkipDedupMessage(message.body);
  const shouldCompleteCategoryRun = !isFocusedSkipDedup;
  const shouldWarmCategoryCache = skipDedup === true && shouldCompleteCategoryRun;
  if (await ackIfFocusedFullAlreadyComplete(message, env)) return;
  if (
    isFocusedSkipDedupMessage(message.body) &&
    !(await claimFocusedFullOrRetry(message, message.body, env))
  )
    return;
  if (!skipDedup) {
    const claimed = await claimRun({ category, env, runYmd });
    if (!claimed.proceed) {
      debugLog(
        message.body,
        `[predict-queue] category-claim skipped ${describePredictMessage(message.body)} state=${
          claimed.state ?? "-"
        }`,
      );
      message.ack();
      return;
    }
    debugLog(
      message.body,
      `[predict-queue] category-claim ok ${describePredictMessage(message.body)}`,
    );
  }
  const predictDoName = buildPredictDoName({
    category,
    keibajoCode,
    raceBango,
    runYmd,
  });
  const predictUrl = buildPredictUrl({
    category,
    daysAhead,
    debug: message.body.debug,
    keibajoCode,
    mode,
    raceBango,
    runYmd,
  });
  const doId = env.FINISH_POSITION_PREDICT_CONTAINER.idFromName(predictDoName);
  const stub = env.FINISH_POSITION_PREDICT_CONTAINER.get(doId);
  try {
    debugLog(
      message.body,
      `[predict-queue] container-fetch start ${describePredictMessage(
        message.body,
      )} doName=${predictDoName} url=${predictUrl}`,
    );
    const response = await stub.fetch(new Request(predictUrl));
    debugLog(
      message.body,
      `[predict-queue] container-fetch response ${describePredictMessage(
        message.body,
      )} doName=${predictDoName} status=${response.status} ok=${response.ok} durationMs=${
        Date.now() - startedAt
      }`,
    );
    if (!response.body) throw new Error("Empty response from predict DO");
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Container DO returned ${response.status}: ${text}`);
    }
    const result = await parseNdjsonStream(response.body, {
      onProgress(line) {
        logPredictProgress(message.body, line);
      },
    });
    debugLog(
      message.body,
      `[predict-queue] container-result ${describePredictMessage(message.body)} status=${
        result.status ?? "-"
      } racesPredicted=${result.racesPredicted} durationMs=${Date.now() - startedAt}`,
    );
    if (isFocusedSkipDedup && (await handleFocusedFullStatus(message, env, result.status))) return;
    assertPredictResultSucceeded(result);
    if (shouldCompleteCategoryRun) {
      await completeRun({
        category,
        env,
        racesPredicted: result.racesPredicted,
        runYmd,
        status: "success",
      });
    }
    if (isFocusedSkipDedup) {
      await completeFocusedFullRace({
        category,
        env,
        keibajoCode: message.body.keibajoCode,
        raceBango: message.body.raceBango,
        runYmd,
        status: "success",
      });
    }
    message.ack();
    console.log(
      `[predict-queue] ack ${describePredictMessage(message.body)} status=${
        result.status ?? RESULT_SUCCESS_STATUS
      } racesPredicted=${result.racesPredicted} durationMs=${Date.now() - startedAt}`,
    );
    if (isFocusedSkipDedup) {
      warmPredictionCacheForFocusedRace(message.body);
    }
    if (shouldWarmCategoryCache) {
      void warmPredictionCacheForCategory({
        category,
        env,
        runDate: message.body.runDateIso ?? message.body.runDate,
        runYmd,
      });
    }
  } catch (err) {
    console.error(
      `Predict failed for category=${category} runYmd=${runYmd}${raceScopeSuffix(
        keibajoCode,
        raceBango,
      )} durationMs=${Date.now() - startedAt}:`,
      String(err),
    );
    if (shouldCompleteCategoryRun) {
      await completeRun({
        category,
        env,
        racesPredicted: 0,
        runYmd,
        status: "error",
      });
    }
    if (isFocusedSkipDedup) {
      await completeFocusedFullRace({
        category,
        env,
        keibajoCode: message.body.keibajoCode,
        raceBango: message.body.raceBango,
        runYmd,
        status: "error",
      });
    }
    message.retry();
  }
};

export const handleQueue = async (
  batch: MessageBatch<PredictQueueMessage>,
  env: Env,
): Promise<void> => {
  for (const message of batch.messages) {
    await processMessage(message, env);
  }
};
