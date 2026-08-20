// Run with bun. Queue consumer: processes one predict message per batch invocation.
// For each message: dedup via DO coordinator (strong consistency), call the Container
// DO stub's fetch, track state.

import { consumeDayBasePickup, isDayBasePickupQueueMessage } from "./day-base-pickup";
import {
  consumeDeliveryCanary,
  isDeliveryCanaryQueueMessage,
  isPredictQueueMessage,
} from "./delivery-canary";
import { recordDeliveryConsumed, recordPredictionCompleted } from "./delivery-lifecycle";
import {
  CONTAINER_SLOT_RETRY_DELAY_SECONDS,
  CONTAINER_SLOT_STALE_MS,
  type ContainerSlotKind,
} from "./container-slot-cap";
import {
  claimContainerSlot,
  claimFocusedFullRace,
  claimRun,
  completeFocusedFullRace,
  completeRun,
  releaseContainerSlot,
  touchContainerSlot,
} from "./do-state";
import { isFocusedFullPredictionComplete } from "./focused-full-completion";
import { pickUpFocusedFullCache } from "./focused-full-cache-pickup";
import {
  parseNdjsonStream,
  type PredictProgressLine,
  type PredictResultLine,
} from "./ndjson-stream";
import { isOldDateRunYmd, OLD_DATE_THRESHOLD_DAYS } from "./old-date-guard";
import {
  buildOldDateSkipEventBindParams,
  buildOldDateSkipEventInsertSql,
  buildOldDateSkipEventRecord,
} from "./old-date-skip-events";
import { hasRequiredPerRaceScope, PER_RACE_SCOPE_REQUIRED_ERROR } from "./per-race-scope-guard";
import {
  buildWarmRaceParamsFromYmd,
  warmPredictionCacheForCategory,
  warmViewerDisplayForRace,
} from "./prediction-cache-warm";
import {
  publishFinishPositionPredictionCache,
  publishFinishPositionPredictionCacheForCategory,
  type PredictionKvPublishResult,
} from "./prediction-kv-writer";
import { parsePredictFailure } from "./predict-failure";
import { resolvePredictDoName } from "./predict-do-shard";
import { resolveCardMaxRaceBangoForKochi } from "./race-coordinator";
import {
  buildRetryErrorBindParams,
  buildRetryErrorInsertSql,
  buildRetryErrorRecord,
} from "./retry-errors";
import type {
  DayBasePickupMessage,
  DeliveryCanaryMessage,
  Env,
  PredictQueueMessage,
} from "./types";

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
// Status written to the focused-full DO claim (predict-run-coordinator.ts)
// when a message is skipped for carrying an old runYmd (old-date-guard.ts).
// Not a TERMINAL_STATUSES entry there, so it does not itself block a future
// claim -- enforcement never depends on this DO write; the guard
// re-evaluates runYmd staleness independently on every message. This write
// is purely observability/bookkeeping.
const OLD_DATE_SKIP_STATUS = "skipped-old-date";
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
// FOCUSED_FULL_RETRY_DELAY_SECONDS (2.5 min) x max_retries (16, set in
// wrangler.jsonc) gives a 40-minute total retry budget per message --
// comfortably above the worst-case observed/extrapolated single-run duration,
// while each individual delivery after launch is a cheap fast completion /
// accepted check (not a re-run) once the in-container single-slot guard sees
// that race already in flight for that category. SLEEP_AFTER in
// container-class.ts is "20m" so a detached first-day day-base build
// (10–15m) is not killed by "Activity expired".
const FOCUSED_FULL_RETRY_DELAY_SECONDS = 150;
// Heartbeat-gap staleness, not "time since claim start": claimFocusedFullRace
// (predict-run-coordinator.ts) refreshes this claim's timestamp on every poll
// that observes the race as still genuinely in flight (not complete, not
// stale), so as long as redeliveries keep landing every
// FOCUSED_FULL_RETRY_DELAY_SECONDS (~2.5 min), this window never trips for a
// legitimately slow pipeline -- it only trips once redeliveries stop, which
// happens either because the race finished/errored (claim moved to a
// terminal/error state) or because retries were exhausted and the message
// landed in the dead-letter queue (see dlq-consumer.ts). Because it is now a
// heartbeat gap rather than an absolute pipeline-duration ceiling, it can be
// set far below the worst-case single-run duration above: 15 minutes is 6x
// the nominal poll cadence (generous margin for Cloudflare Queues redelivery
// jitter) while staying well under the 40-minute retry budget, so a message
// that stops being redelivered (queue misbehavior, not the pipeline itself)
// still gets 25 minutes / ~10 further retries of in-band reclaim attempts
// before it would ever reach the dead-letter queue. This closes the defect
// where the old 35-minute absolute window exceeded the old 30-minute retry
// budget, so a genuinely dead run could never be reclaimed in-band before
// its message was dead-lettered into a queue with zero consumer.
const FOCUSED_FULL_IN_FLIGHT_STALE_MS = 15 * 60 * 1000;
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
const RESCORE_SLOT_KIND: ContainerSlotKind = "rescore";
const FOCUSED_FULL_SLOT_KIND: ContainerSlotKind = "focused-full";
// Focused-full "accepted" keeps this delivery's slot lease: the detached
// pipeline is still occupying the unique DO after the HTTP response ends.
// The next redelivery may claim the same doName again (holders++) instead of
// starting another instance; a later success / already-complete / Neon-complete
// path releases once (holders--). If redeliveries stop, CONTAINER_SLOT_STALE_MS
// (20m) prunes the leftover lease.
const FOCUSED_FULL_ACCEPTED_KEEP_SLOT: boolean = true;

interface PredictUrlParams {
  category: string;
  daysAhead: number;
  debug?: boolean;
  // Forwarded to the container so its own row-count-only completion check
  // (_focused_full_is_complete, predict_lib/serve.py) can be bypassed the
  // same way force already bypasses this Worker's ackIfFocusedFullAlreadyComplete
  // -- see Defect H, apps/pc-keiba-viewer/docs/probes/
  // jra-serving-audit-jun-jul-2026-07-17.md. Absent/false keeps the
  // container's own guard active.
  force?: boolean;
  keibajoCode?: string;
  mode: string;
  raceBango?: string;
  // runYmd is the YYYYMMDD date string required by the container /predict endpoint.
  runYmd: string;
  // Registered-card-max race_bango for the is_final_race cell-routing
  // dimension (predict_lib.cell_router.py). Only ever set for a single-race
  // scoped request (keibajoCode present) -- see resolveCardMaxRaceBangoForKochi.
  cardMaxRaceBango?: number;
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

interface ViewerDisplayWarmTarget {
  keibajoCode: string;
  raceBango: string;
  runYmd: string;
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
  cardMaxRaceBango?: number;
}

interface ClaimContainerSlotOrRetryParams {
  category: string;
  doName: string;
  env: Env;
  kind: ContainerSlotKind;
  message: Message<PredictQueueMessage>;
}

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
  if (params.force === true) searchParams.set("force", "1");
  if (params.cardMaxRaceBango !== undefined) {
    searchParams.set("cardMaxRaceBango", String(params.cardMaxRaceBango));
  }
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
  if (params.cardMaxRaceBango !== undefined) {
    searchParams.set("cardMaxRaceBango", String(params.cardMaxRaceBango));
  }
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

// Exported for reuse by dlq-consumer.ts, which needs the same narrowing to
// decide whether a dead-lettered message holds a focused-full DO claim that
// must be force-unstuck before redriving it.
export const isFocusedSkipDedupMessage = (
  message: PredictQueueMessage,
): message is FocusedFullSkipDedupMessage =>
  message.skipDedup === true &&
  message.mode === "full" &&
  message.keibajoCode !== undefined &&
  message.raceBango !== undefined;

const warmViewerDisplayAfterKvWrite = async (
  published: PredictionKvPublishResult,
  params: ViewerDisplayWarmTarget,
): Promise<void> => {
  if (published.status !== "written") return;
  await warmViewerDisplayForRace(
    buildWarmRaceParamsFromYmd(params.runYmd, params.keibajoCode, params.raceBango),
  );
};

const populateViewerDisplayForFocusedRace = async (
  env: Env,
  message: FocusedFullSkipDedupMessage,
): Promise<void> => {
  const published = await publishPredictionKvForFocusedRace(env, message, false);
  await warmViewerDisplayAfterKvWrite(published, message);
};

const publishPredictionKvForRace = async (
  env: Env,
  params: {
    bustCacheApi: boolean;
    category: PredictQueueMessage["category"];
    keibajoCode: string;
    raceBango: string;
    runYmd: string;
  },
): Promise<PredictionKvPublishResult> => {
  const result = await publishFinishPositionPredictionCache({ env, ...params });
  console.log(
    `prediction kv fp publish category=${params.category} runYmd=${params.runYmd} keibajo=${params.keibajoCode} race=${params.raceBango} status=${result.status} busted=${result.busted}`,
  );
  return result;
};

const publishPredictionKvForFocusedRace = (
  env: Env,
  message: FocusedFullSkipDedupMessage,
  bustCacheApi: boolean,
): Promise<PredictionKvPublishResult> =>
  publishPredictionKvForRace(env, {
    bustCacheApi,
    category: message.category,
    keibajoCode: message.keibajoCode,
    raceBango: message.raceBango,
    runYmd: message.runYmd,
  });

const ackIfFocusedFullAlreadyComplete = async (
  message: Message<PredictQueueMessage>,
  env: Env,
): Promise<boolean> => {
  if (!isFocusedSkipDedupMessage(message.body)) return false;
  if (message.body.force === true) {
    debugLog(
      message.body,
      `[predict-queue] focused-completion-check bypassed (force) ${describePredictMessage(message.body)}`,
    );
    return false;
  }
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
    // Neon confirms the race is done, but the detached pipeline that produced
    // it never had a live HTTP response to embed its R2 feat-cache payload
    // into (see focused-full-cache-pickup.ts's module docstring). This is the
    // one point in the redelivery flow that observes completion without ever
    // re-fetching the container, so it is also the only place a pickup can
    // happen. Errors are swallowed inside pickUpFocusedFullCache -- a missed
    // pickup must never block the ack below.
    await pickUpFocusedFullCache({
      category,
      debug: message.body.debug,
      env,
      keibajoCode,
      raceBango,
      runYmd,
    });
    debugLog(
      message.body,
      `Skipping focused full already complete category=${category} runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango}`,
    );
    await recordCompletedBestEffort(env, message.body);
    message.ack();
    await populateViewerDisplayForFocusedRace(env, message.body);
    await releaseContainerSlotBestEffort(
      env,
      resolvePredictDoName({
        category,
        env,
        keibajoCode,
        raceBango,
      }),
      FOCUSED_FULL_SLOT_KIND,
    );
    return true;
  } catch (err) {
    console.warn(
      `Focused full completion guard failed category=${category} runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango}:`,
      String(err),
    );
    return false;
  }
};

const recordConsumedBestEffort = async (env: Env, message: PredictQueueMessage): Promise<void> => {
  try {
    await recordDeliveryConsumed(env, message, new Date());
  } catch (error) {
    console.error("[predict-queue] failed to record consumed lifecycle", String(error));
  }
};

const recordCompletedBestEffort = async (env: Env, message: PredictQueueMessage): Promise<void> => {
  try {
    await recordPredictionCompleted(env, message, new Date());
  } catch (error) {
    console.error("[predict-queue] failed to record completed lifecycle", String(error));
  }
};

const releaseContainerSlotBestEffort = async (
  env: Env,
  doName: string,
  kind: ContainerSlotKind,
): Promise<void> => {
  try {
    await releaseContainerSlot({ doName, env, kind });
  } catch (err) {
    console.error(
      `[predict-queue] failed to release container slot doName=${doName} kind=${kind}:`,
      String(err),
    );
  }
};

const touchContainerSlotBestEffort = async (env: Env, doName: string): Promise<void> => {
  try {
    await touchContainerSlot({ doName, env, staleAfterMs: CONTAINER_SLOT_STALE_MS });
  } catch (err) {
    console.error(`[predict-queue] failed to touch container slot doName=${doName}:`, String(err));
  }
};

const claimContainerSlotOrRetry = async (
  params: ClaimContainerSlotOrRetryParams,
): Promise<boolean> => {
  const claim = await claimContainerSlot({
    category: params.category,
    doName: params.doName,
    env: params.env,
    kind: params.kind,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
  });
  if (claim.proceed) return true;
  console.warn(
    `[predict-queue] container slot ${claim.state ?? "capped"} doName=${params.doName} kind=${
      params.kind
    } ${describePredictMessage(params.message.body)} -- will retry without starting a container`,
  );
  params.message.retry({ delaySeconds: CONTAINER_SLOT_RETRY_DELAY_SECONDS });
  return false;
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
    force: body.force === true,
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
  await touchContainerSlotBestEffort(
    env,
    resolvePredictDoName({
      category,
      env,
      keibajoCode,
      raceBango,
    }),
  );
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

const optionalQueueAttempts = (message: Message<PredictQueueMessage>): number | undefined =>
  typeof message.attempts === "number" ? message.attempts : undefined;

const optionalQueueMessageId = (message: Message<PredictQueueMessage>): string | undefined =>
  typeof message.id === "string" && message.id.length > 0 ? message.id : undefined;

// Cloudflare Queues message.retry() redelivers the original body unchanged, so
// the failure that will eventually dead-letter this message must be persisted
// out-of-band. Best-effort: a D1 hiccup must not itself block the retry.
const persistRetryError = async (
  env: Env,
  message: Message<PredictQueueMessage>,
  err: unknown,
): Promise<void> => {
  try {
    const failure = parsePredictFailure(err);
    const record = buildRetryErrorRecord({
      ...failure,
      category: message.body.category,
      keibajoCode: message.body.keibajoCode,
      mode: message.body.mode,
      queueAttempts: optionalQueueAttempts(message),
      queueMessageId: optionalQueueMessageId(message),
      raceBango: message.body.raceBango,
      runYmd: message.body.runYmd,
    });
    await env.FINISH_POSITION_CRON_DB.prepare(buildRetryErrorInsertSql())
      .bind(...buildRetryErrorBindParams(record))
      .run();
  } catch (persistErr) {
    console.error(
      `[predict-queue] failed to persist retry error ${describePredictMessage(message.body)}:`,
      String(persistErr),
    );
  }
};

const retryAfterFailure = async (
  message: Message<PredictQueueMessage>,
  env: Env,
  err: unknown,
): Promise<void> => {
  await persistRetryError(env, message, err);
  message.retry();
};

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
    const busyExhausted = new Error(
      `Focused full slot busy budget exhausted busyRequeueCount=${currentCount}`,
    );
    busyExhausted.name = "BusyRequeueExhausted";
    await retryAfterFailure(message, env, busyExhausted);
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
      await pickUpFocusedFullCache({
        category,
        debug: message.body.debug,
        env,
        keibajoCode: message.body.keibajoCode,
        raceBango: message.body.raceBango,
        runYmd,
      });
    }
    await recordCompletedBestEffort(env, message.body);
    message.ack();
    if (isFocusedSkipDedupMessage(message.body)) {
      await populateViewerDisplayForFocusedRace(env, message.body);
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
// are swallowed inside the warm helper. KV write-through is awaited so the
// queue isolate cannot exit before pred:fp settles; publish status is logged.
const processContainerPerRaceRescore = async (
  message: Message<PerRaceRescoreMessage>,
  env: Env,
): Promise<void> => {
  const { category, daysAhead, debug, keibajoCode, raceBango, runYmd } = message.body;
  const startedAt = Date.now();
  const predictDoName = resolvePredictDoName({
    category,
    env,
    keibajoCode,
    raceBango,
    shareCategoryInstance: true,
  });
  if (
    !(await claimContainerSlotOrRetry({
      category,
      doName: predictDoName,
      env,
      kind: RESCORE_SLOT_KIND,
      message,
    }))
  ) {
    return;
  }
  const cardMaxRaceBango = await resolveCardMaxRaceBangoForKochi({ env, keibajoCode, runYmd });
  const predictUrl = buildPerRaceRescoreUrl({
    cardMaxRaceBango,
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
    const published = await publishPredictionKvForRace(env, {
      bustCacheApi: true,
      category,
      keibajoCode,
      raceBango,
      runYmd,
    });
    await warmViewerDisplayAfterKvWrite(published, { keibajoCode, raceBango, runYmd });
  } catch (err) {
    console.error(
      `Container per-race rescore failed category=${category} runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango} durationMs=${
        Date.now() - startedAt
      }:`,
      String(err),
    );
    await retryAfterFailure(message, env, err);
  } finally {
    await releaseContainerSlotBestEffort(env, predictDoName, RESCORE_SLOT_KIND);
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

// Handles a message whose runYmd is too old to dispatch to the Container
// (old-date-guard.ts). Writes a durable skip-event row and, for a focused
// per-race full skipDedup message, marks the DO claim complete with
// status=skipped-old-date so it stops being treated as in-flight. Both are
// wrapped in one try/catch so a bookkeeping failure (D1 write or DO call)
// never prevents the ack() -- an old message must never get stuck retrying
// just because the audit write hiccuped -- but the failure is still logged.
const handleOldDateSkip = async (
  message: Message<PredictQueueMessage>,
  env: Env,
): Promise<void> => {
  const { category, keibajoCode, mode, raceBango, runYmd } = message.body;
  try {
    const record = buildOldDateSkipEventRecord({
      category,
      keibajoCode,
      mode,
      raceBango,
      runYmd,
      thresholdDays: OLD_DATE_THRESHOLD_DAYS,
    });
    await env.FINISH_POSITION_CRON_DB.prepare(buildOldDateSkipEventInsertSql())
      .bind(...buildOldDateSkipEventBindParams(record))
      .run();
    if (isFocusedSkipDedupMessage(message.body)) {
      await completeFocusedFullRace({
        category,
        env,
        keibajoCode: message.body.keibajoCode,
        raceBango: message.body.raceBango,
        runYmd,
        status: OLD_DATE_SKIP_STATUS,
      });
    }
  } catch (err) {
    console.error(
      `Old-date skip bookkeeping failed ${describePredictMessage(message.body)}:`,
      String(err),
    );
  }
  message.ack();
  console.warn(
    `Skipping old-dated predict message ${describePredictMessage(
      message.body,
    )} thresholdDays=${OLD_DATE_THRESHOLD_DAYS}`,
  );
};

const handleMissingPerRaceScopeSkip = (message: Message<PredictQueueMessage>): void => {
  console.warn(
    `Skipping day-scoped predict message ${describePredictMessage(message.body)}: ${PER_RACE_SCOPE_REQUIRED_ERROR}`,
  );
  message.ack();
};

const processMessage = async (message: Message<PredictQueueMessage>, env: Env): Promise<void> => {
  await recordConsumedBestEffort(env, message.body);
  if (
    !hasRequiredPerRaceScope({
      keibajoCode: message.body.keibajoCode,
      raceBango: message.body.raceBango,
    })
  ) {
    handleMissingPerRaceScopeSkip(message);
    return;
  }
  if (message.body.force !== true && isOldDateRunYmd(message.body.runYmd, new Date()))
    return handleOldDateSkip(message, env);
  debugLog(message.body, `[predict-queue] received ${describePredictMessage(message.body)}`);
  if (isPerRaceRescore(message)) return processPerRaceRescore(message, env);
  const { category, runYmd, daysAhead, mode, keibajoCode, raceBango, skipDedup } = message.body;
  const startedAt = Date.now();
  const isFocusedSkipDedup = isFocusedSkipDedupMessage(message.body);
  const shouldCompleteCategoryRun = !isFocusedSkipDedup;
  const shouldWarmCategoryCache =
    skipDedup === true && shouldCompleteCategoryRun && mode !== RESCORE_MODE;
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
  const predictDoName = resolvePredictDoName({ category, env, keibajoCode, raceBango });
  if (
    !(await claimContainerSlotOrRetry({
      category,
      doName: predictDoName,
      env,
      kind: FOCUSED_FULL_SLOT_KIND,
      message,
    }))
  ) {
    return;
  }
  const cardMaxRaceBango = await resolveCardMaxRaceBangoForKochi({ env, keibajoCode, runYmd });
  const predictUrl = buildPredictUrl({
    cardMaxRaceBango,
    category,
    daysAhead,
    debug: message.body.debug,
    force: message.body.force,
    keibajoCode,
    mode,
    raceBango,
    runYmd,
  });
  const doId = env.FINISH_POSITION_PREDICT_CONTAINER.idFromName(predictDoName);
  const stub = env.FINISH_POSITION_PREDICT_CONTAINER.get(doId);
  const slotHold = { keep: false };
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
    if (isFocusedSkipDedup && (await handleFocusedFullStatus(message, env, result.status))) {
      slotHold.keep =
        result.status === FOCUSED_FULL_ACCEPTED_STATUS && FOCUSED_FULL_ACCEPTED_KEEP_SLOT;
      return;
    }
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
      // Detached focused-full already returned "accepted" before the parquet
      // existed. Pickup here is the success-path counterpart of the Neon-complete
      // skip path above: pull GET /focused-full-cache and FEATURES_CACHE.put.
      await pickUpFocusedFullCache({
        category,
        debug: message.body.debug,
        env,
        keibajoCode: message.body.keibajoCode,
        raceBango: message.body.raceBango,
        runYmd,
      });
    }
    await recordCompletedBestEffort(env, message.body);
    message.ack();
    console.log(
      `[predict-queue] ack ${describePredictMessage(message.body)} status=${
        result.status ?? RESULT_SUCCESS_STATUS
      } racesPredicted=${result.racesPredicted} durationMs=${Date.now() - startedAt}`,
    );
    if (isFocusedSkipDedup) {
      await populateViewerDisplayForFocusedRace(env, message.body);
    }
    // Non-skipDedup per-race full still uses category completeRun and does not
    // publish pred:fp here. Generate-time display warm is skipDedup focused-full
    // plus per-race rescore only. Do not revive shouldWarmCategoryCache
    // (warm-before-KV); day-base pickup also does not write pred:fp.
    if (shouldWarmCategoryCache) {
      void warmPredictionCacheForCategory({
        category,
        env,
        runDate: message.body.runDateIso ?? message.body.runDate,
        runYmd,
      });
      void publishFinishPositionPredictionCacheForCategory({
        bustCacheApi: false,
        category,
        env,
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
    await retryAfterFailure(message, env, err);
  } finally {
    if (!slotHold.keep) {
      await releaseContainerSlotBestEffort(env, predictDoName, FOCUSED_FULL_SLOT_KIND);
    }
  }
};

export const handleQueue = async (
  batch: MessageBatch<PredictQueueMessage | DeliveryCanaryMessage | DayBasePickupMessage>,
  env: Env,
): Promise<void> => {
  for (const message of batch.messages) {
    if (isDeliveryCanaryQueueMessage(message)) {
      await consumeDeliveryCanary(env, message.body, new Date());
      message.ack();
    } else if (isDayBasePickupQueueMessage(message)) {
      await consumeDayBasePickup({ env, message: message.body });
      message.ack();
    } else if (isPredictQueueMessage(message)) {
      await processMessage(message, env);
    }
  }
};
