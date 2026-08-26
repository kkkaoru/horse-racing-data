// Run with bun. Queue consumer: processes one predict message per batch invocation.
// For each message: dedup via DO coordinator (strong consistency), call the Container
// DO stub's fetch, track state.

import {
  cleanupDayBaseWork,
  consumeDayBasePickup,
  dayBaseGenerationFields,
  isDayBasePickupQueueMessage,
} from "./day-base-pickup";
import {
  consumeDeliveryCanary,
  isDeliveryCanaryQueueMessage,
  isPredictQueueMessage,
} from "./delivery-canary";
import {
  recordDeliveryConsumed,
  recordPredictionCompleted,
  recordPreweightDisplayCompleted,
  recordPreweightGenerationStarted,
} from "./delivery-lifecycle";
import {
  CONTAINER_SLOT_CAPPED_STATE,
  CONTAINER_SLOT_RETRY_DELAY_SECONDS,
  CONTAINER_SLOT_STALE_MS,
  type ContainerSlotKind,
} from "./container-slot-cap";
import { consumeContainerStop } from "./container-control";
import {
  consumeContainerCleanup,
  handOffContainerStopOrCleanup,
  isContainerCleanupQueueMessage,
} from "./container-cleanup";
import { isDayBasePrewarmQueueMessage, prewarmCategoryWithOutcome } from "./day-base-prewarm";
import {
  cancelFocusedFullRaceRepair,
  claimFocusedFullTerminalWatch,
  claimContainerSlot,
  claimFocusedFullRace,
  claimRescoreExecution,
  claimRun,
  clearContainerSlot,
  completeFocusedFullRace,
  completeFocusedFullTerminalWatch,
  completeRescoreRace,
  completeRun,
  releaseContainerSlot,
  markFocusedFullTerminalWatchStopped,
  reserveFocusedFullRaceRepair,
  touchContainerSlot,
} from "./do-state";
import {
  isFocusedFullPredictionComplete,
  isPerRaceFeatureCachePresent,
  isPerRaceRescoreReady,
} from "./focused-full-completion";
import { pickUpFocusedFullCache } from "./focused-full-cache-pickup";
import {
  buildFocusedFullStatusUrl as buildWatchStatusUrl,
  FOCUSED_FULL_WATCH_BACKUP_SECONDS,
  FOCUSED_FULL_WATCH_POLL_SECONDS,
  pollFocusedFullWatchTick,
  sendFocusedFullWatchMessageDurably,
  WATCH_REQUEST_HEADER,
  WATCH_RESPONSE_HEADER,
} from "./focused-full-watch";
import { clearDayBaseRepairReservation, enqueueDayBaseRepairOnce } from "./day-base-repair";
import {
  getDayBaseRaceFoundationReadiness,
  materializeDayBasePerRaceCache,
} from "./day-base-race-materializer";
import { getFocusedFullDayBaseReadiness } from "./focused-full-day-base-readiness";
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
import {
  hasRequiredPerRaceScope,
  hasValidPerRaceScope,
  PER_RACE_SCOPE_INVALID_ERROR,
  PER_RACE_SCOPE_REQUIRED_ERROR,
} from "./per-race-scope-guard";
import { buildWarmRaceParamsFromYmd, warmViewerDisplayForRace } from "./prediction-cache-warm";
import {
  publishFinishPositionPredictionCache,
  type PredictionKvPublishResult,
} from "./prediction-kv-writer";
import { parsePredictFailure } from "./predict-failure";
import { resolvePredictDoName } from "./predict-do-shard";
import {
  qualifyPredictionContainerDoName,
  resolveContainerNamespaceForRole,
  resolveRaceContainerRoute,
  type PredictionContainerRole,
} from "./race-container-routing";
import { resolveCardMaxRaceBangoForKochi } from "./race-coordinator";
import {
  addMarketSignalAttestationToUrl,
  prepareMarketSignalFoundationBestEffort,
} from "./race-chain-market-signal-hook";
import { isBeforeRaceStartDeadline, RaceDeadlineExceededError } from "./race-deadline";
import { addRescoreAttestationToUrl, createRescoreAttestation } from "./rescore-attestation";
import { rescoreJraRace } from "./scoring/rescore-consumer";
import {
  buildRetryErrorBindParams,
  buildRetryErrorInsertSql,
  buildRetryErrorRecord,
} from "./retry-errors";
import type {
  ContainerControlMessage,
  DayBasePrewarmMessage,
  Env,
  FocusedFullCompletionMessage,
  FocusedFullWatchTickMessage,
  FocusedFullWatchPayload,
  PredictCategory,
  PredictionCacheRepairMessage,
  PredictQueueBody,
  PredictQueueMessage,
} from "./types";

const PREDICT_PATH = "/predict";
const FOCUSED_FULL_STATUS_PATH = "/focused-full-status";
const PREDICT_HOST = "http://do";
const RESCORE_MODE = "rescore";
const RESULT_SUCCESS_STATUS = "success";
const DAY_BASE_REQUIRED_ERROR_CODE = "DAY_BASE_REQUIRED:";
const ENABLED_FLAG = "1";
// A terminal Worker invocation can legally run for up to 15 minutes. Keep the
// ownership lease beyond that bound so the watchdog chain never steals while
// the original cache/stop finalizer can still be executing.
const FOCUSED_FULL_TERMINAL_CLAIM_STALE_MS = 16 * 60 * 1000;
// Focused full returns "accepted" when the container either launched a detached
// pipeline for this race or observed the same race already in flight. The queue
// consumer polls Neon completion on delayed redeliveries instead of holding a
// Cloudflare request open for the whole feature chain.
const FOCUSED_FULL_ACCEPTED_STATUS = "accepted";
const FOCUSED_FULL_BUSY_STATUS = "busy";
const FOCUSED_FULL_ALREADY_COMPLETE_STATUS = "already-complete";
const PREDICTION_CACHE_REPAIR_RETRY_DELAY_SECONDS = 30;
const CONTAINER_SLOT_STOPPING_STATE = "stopping";
// Status written to the focused-full DO claim (predict-run-coordinator.ts)
// when a message is skipped for carrying an old runYmd (old-date-guard.ts).
// Not a TERMINAL_STATUSES entry there, so it does not itself block a future
// claim -- enforcement never depends on this DO write; the guard
// re-evaluates runYmd staleness independently on every message. This write
// is purely observability/bookkeeping.
const OLD_DATE_SKIP_STATUS = "skipped-old-date";
// The container returns "busy" when another race in the same category holds its
// single per-process pipeline slot. Retry the original Queue message so its
// identity and delivery-attempt budget remain intact and a permanently stuck
// slot eventually reaches the DLQ. The retry delay grows with message.attempts
// (see computeRetryDelaySeconds below) rather than staying fixed: each
// redelivery re-runs the Neon focused-full
// completion guard, so a short delay while the slot is likely to free up soon
// and a longer delay once many races are already queued ahead keeps
// slot-pickup latency low without scaling Neon query volume linearly with
// how deep in a same-day burst this message sits.
const BUSY_RETRY_DELAY_BASE_SECONDS = 30;
const BUSY_RETRY_DELAY_STEP_SECONDS = 20;
const BUSY_RETRY_DELAY_MAX_SECONDS = 300;
const LEGACY_BUSY_REQUEUE_COUNT_ZERO = 0;
// Retry budget reasoning: the Python container's focused-full pipeline
// (DuckDB base build + 10-17 sequential layer scripts +
// CatBoost/XGBoost scoring + Neon UPSERT) has been observed taking 10-20+
// minutes end-to-end (NAR ~13-17 min extrapolated from partial timing; JRA has
// ~1.6x as many layers so plausibly up to ~25-27 min).
// FOCUSED_FULL_RETRY_DELAY_SECONDS (30 sec) x max_retries (100, set in
// wrangler.jsonc) gives a bounded retry budget with ample race-day headroom --
// comfortably above the worst-case observed/extrapolated single-run duration,
// while each individual delivery after launch is a cheap fast completion /
// accepted check (not a re-run) once the in-container single-slot guard sees
// that race already in flight for that category. SLEEP_AFTER in
// container-class.ts is "20m" so a detached first-day day-base build
// (10–15m) is not killed by "Activity expired".
const FOCUSED_FULL_RETRY_DELAY_SECONDS = 30;
// Absolute no-progress deadline for a launched race. Queue redelivery alone
// must not refresh it: receiving the same message is not evidence that the
// detached Python pipeline is alive. Once this expires the Worker clears the
// matching slot owner and probes the Container again; its same-race guard
// prevents a duplicate when Python is still genuinely running.
// Python owns a 30-minute absolute pipeline deadline. The coordinator must
// never rotate a healthy owner before that authoritative deadline; one extra
// minute lets the next status poll observe deadlineExpired and recycle it.
const FOCUSED_FULL_IN_FLIGHT_STALE_MS = 31 * 60 * 1000;
// Python emits a heartbeat while the detached pipeline is alive. A running
// status without that heartbeat is a crashed/evicted Container, not evidence
// that the 31-minute absolute deadline should be extended indefinitely.
const FOCUSED_FULL_PROGRESS_STALE_MS = 4 * 60 * 1000;
const RESCORE_EXECUTION_STALE_MS = 31 * 60 * 1000;
const RESCORE_NEAR_POST_THRESHOLD_SECONDS = 5 * 60;
const RESCORE_NEAR_POST_RETRY_MAX_SECONDS = 15;
const MIN_RETRY_DELAY_SECONDS = 1;
const PREDICTION_CACHE_REPAIR_MAX_ATTEMPTS = 4;
const INACTIVE_CONTAINER_DO_ERROR_PREFIX =
  "Connection closed: this Durable Object instance is no longer active.";
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

interface TerminalContainerStopParams {
  doName: string;
  env: Env;
  role: PredictionContainerRole;
  workKey: string;
}

interface FocusedFullCachePickupParams {
  env: Env;
  message: FocusedFullSkipDedupMessage;
  role: PredictionContainerRole;
}

interface PastDayBaseWorkParams {
  category: PredictCategory;
  env: Env;
  generationId?: string;
  runYmd: string;
}

type FocusedFullPollStatus = "error" | "missing" | "running" | "success";

interface FocusedFullStatusPayload {
  error: string | null;
  lastProgressAtMs: number | null;
  raceKey: string;
  status: FocusedFullPollStatus;
}

interface RecoverFocusedFullStatusParams {
  body: FocusedFullSkipDedupMessage;
  doName: string;
  env: Env;
  error: string;
  message: Message<PredictQueueMessage>;
  role: PredictionContainerRole;
  synchronousStop?: boolean;
  workKey: string;
}

interface CompleteFocusedFullFromStatusParams {
  body: FocusedFullSkipDedupMessage;
  doName: string;
  env: Env;
  message: Message<PredictQueueMessage>;
  role: PredictionContainerRole;
  synchronousStop?: boolean;
  workKey: string;
}

interface FocusedFullStatusTarget {
  doName: string;
  getContainerState: () => Promise<string>;
  fetchStatus: () => Promise<FocusedFullStatusPayload>;
  role: PredictionContainerRole;
  workKey: string;
}

interface FocusedFullStatusCheck extends FocusedFullStatusTarget {
  status: FocusedFullStatusPayload;
}

interface EnqueueFocusedFullCacheRepairParams {
  body: FocusedFullSkipDedupMessage;
  check: FocusedFullStatusCheck;
  env: Env;
  message: Message<PredictQueueMessage>;
}

type FocusedFullCacheRepairReason = "cache-missing-after-success" | "missing-status";

interface FocusedFullCacheRepairRequest {
  body: FocusedFullSkipDedupMessage;
  env: Env;
  message: Message<PredictQueueMessage>;
  reason: FocusedFullCacheRepairReason;
}

interface HandleFocusedFullStatusParams {
  doName: string;
  env: Env;
  message: Message<PredictQueueMessage>;
  role: PredictionContainerRole;
  slotHold: { keep: boolean };
  status: string | undefined;
  watchId?: string;
  workKey: string;
}

interface DeferredMessageDecision {
  acknowledged: boolean;
  retryOptions?: Parameters<Message<PredictQueueMessage>["retry"]>[0];
  retried: boolean;
}
// Focused-full "accepted" keeps this delivery's slot lease: the detached
// pipeline is still occupying the unique DO after the HTTP response ends.
// A later success / already-complete / Neon-complete path releases the exact
// work-owned lease. Redeliveries observe the race claim without incrementing
// holders or refreshing the lease merely because the Queue delivered again.
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

interface ViewerPredictionCacheTarget extends ViewerDisplayWarmTarget {
  category: PredictCategory;
}

interface RescoreDisplayTarget extends ViewerPredictionCacheTarget {
  raceStartAtJst: string | undefined;
}

type ViewerPredictionCacheRepairOutcome = "complete" | "outside-window" | "retry";

interface PredictionCacheRepairEnqueueOptions {
  delaySeconds: number;
}

const toViewerPredictionCacheTarget = (
  target: ViewerPredictionCacheTarget,
): ViewerPredictionCacheTarget => ({
  category: target.category,
  keibajoCode: target.keibajoCode,
  raceBango: target.raceBango,
  runYmd: target.runYmd,
});

interface PerRaceRescoreUrlParams {
  activeHorseNumbers?: number[];
  category: string;
  daysAhead: number;
  debug?: boolean;
  // keibajoCode / raceBango are 2-digit zero-padded strings from the per-race coordinator.
  keibajoCode: string;
  raceBango: string;
  raceStartAtJst: string;
  // runYmd is the YYYYMMDD date string required by the container /predict endpoint.
  runYmd: string;
  cardMaxRaceBango?: number;
  entrySnapshotFetchedAt?: string;
  entrySnapshotHash?: string;
  excludedHorseNumbers?: number[];
  weightSnapshotCount: number;
  weightSnapshotFetchedAt: string;
  weightSnapshotHash: string;
}

interface ClaimContainerSlotOrRetryParams {
  category: string;
  doName: string;
  env: Env;
  kind: ContainerSlotKind;
  message: Message<PredictQueueMessage>;
  workKey: string;
}

interface ReleaseContainerSlotBestEffortParams {
  doName: string;
  env: Env;
  kind: ContainerSlotKind;
  workKey: string;
}

interface ContainerRequestLifecycle {
  started: boolean;
  terminal: boolean;
}

interface FinishExpiredRescoreInput {
  env: Env;
  message: Message<PerRaceRescoreMessage>;
  stage: string;
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
    raceStartAtJst: params.raceStartAtJst,
    runDate: params.runYmd,
    weightSnapshotCount: String(params.weightSnapshotCount),
    weightSnapshotFetchedAt: params.weightSnapshotFetchedAt,
    weightSnapshotHash: params.weightSnapshotHash,
  });
  if (params.debug === true) searchParams.set("debug", "1");
  if (params.activeHorseNumbers !== undefined) {
    searchParams.set("activeHorseNumbers", JSON.stringify(params.activeHorseNumbers));
  }
  if (params.excludedHorseNumbers !== undefined) {
    searchParams.set("excludedHorseNumbers", JSON.stringify(params.excludedHorseNumbers));
  }
  if (params.entrySnapshotFetchedAt !== undefined) {
    searchParams.set("entrySnapshotFetchedAt", params.entrySnapshotFetchedAt);
  }
  if (params.entrySnapshotHash !== undefined) {
    searchParams.set("entrySnapshotHash", params.entrySnapshotHash);
  }
  if (params.cardMaxRaceBango !== undefined) {
    searchParams.set("cardMaxRaceBango", String(params.cardMaxRaceBango));
  }
  return `${PREDICT_HOST}${PREDICT_PATH}?${searchParams.toString()}`;
};

const debugLog = (body: Pick<PredictQueueMessage, "debug">, message: string): void => {
  if (body.debug === true) console.log(message);
};

const handOffTerminalContainerStop = async (
  params: TerminalContainerStopParams,
): Promise<boolean> => {
  await handOffContainerStopOrCleanup({
    env: params.env,
    name: params.doName,
    role: params.role,
    workKey: params.workKey,
  });
  return true;
};

const tryHandOffTerminalContainerStop = async (
  params: TerminalContainerStopParams,
): Promise<boolean> => {
  try {
    return await handOffTerminalContainerStop(params);
  } catch (error) {
    console.error(
      `[predict-queue] successful focused-full cleanup handoff failed doName=${params.doName} workKey=${params.workKey}:`,
      String(error),
    );
    return false;
  }
};

export const buildFocusedFullWorkKey = (
  message: Pick<FocusedFullSkipDedupMessage, "category" | "keibajoCode" | "raceBango" | "runYmd">,
): string =>
  `focused-full:${message.runYmd}:${message.category}:${message.keibajoCode}:${message.raceBango}`;

export const buildPredictWorkKey = (message: PredictQueueMessage): string =>
  `${message.mode}:${message.runYmd}:${message.category}:${message.keibajoCode ?? "-"}:${
    message.raceBango ?? "-"
  }`;

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
  env: Env,
  published: PredictionKvPublishResult,
  params: ViewerDisplayWarmTarget,
): Promise<boolean> => {
  if (published.status !== "written" || typeof published.expectedGeneratedAt !== "string") {
    return false;
  }
  const internalToken = env.PC_KEIBA_VIEWER_INTERNAL_TOKEN?.trim();
  if (internalToken === undefined || internalToken.length === 0) return false;
  const warmParams: ReturnType<typeof buildWarmRaceParamsFromYmd> = buildWarmRaceParamsFromYmd(
    params.runYmd,
    params.keibajoCode,
    params.raceBango,
  );
  return warmViewerDisplayForRace({
    ...warmParams,
    expectedGeneratedAt: published.expectedGeneratedAt,
    internalToken,
    refresh: true,
    ...(env.PC_KEIBA_VIEWER === undefined ? {} : { viewer: env.PC_KEIBA_VIEWER }),
  });
};

const warmViewerDisplayBestEffort = async (
  env: Env,
  published: PredictionKvPublishResult,
  target: ViewerDisplayWarmTarget,
  category?: PredictCategory,
): Promise<boolean> => {
  try {
    if (await warmViewerDisplayAfterKvWrite(env, published, target)) return true;
    console.warn(
      `[predict-queue] viewer cache warm best-effort failed category=${category === undefined ? "-" : category} runYmd=${target.runYmd} keibajo=${target.keibajoCode} race=${target.raceBango}: returned-false`,
    );
  } catch (error) {
    console.warn(
      `[predict-queue] viewer cache warm best-effort failed category=${category === undefined ? "-" : category} runYmd=${target.runYmd} keibajo=${target.keibajoCode} race=${target.raceBango}: ${String(error)}`,
    );
  }
  return false;
};

const publishAndWarmRescoreDisplay = async (
  env: Env,
  target: RescoreDisplayTarget,
): Promise<boolean> => {
  if (
    !isBeforeRaceStartDeadline({
      nowMs: Date.now(),
      raceStartAtJst: target.raceStartAtJst,
    })
  )
    return false;
  const published = await publishPredictionKvForRace(env, {
    bustCacheApi: true,
    category: target.category,
    keibajoCode: target.keibajoCode,
    raceBango: target.raceBango,
    runYmd: target.runYmd,
  });
  if (published.status !== "written") {
    throw new Error(`Prediction KV publish did not write: ${published.status}`);
  }
  if (!published.busted) throw new Error("Prediction viewer cache bust failed");
  if (!(await warmViewerDisplayBestEffort(env, published, target, target.category))) {
    try {
      await enqueuePredictionCacheRepair(env, target, {
        delaySeconds: PREDICTION_CACHE_REPAIR_RETRY_DELAY_SECONDS,
      });
      console.warn(
        `[predict-queue] deferred prediction cache repair after warm miss category=${target.category} runYmd=${target.runYmd} keibajo=${target.keibajoCode} race=${target.raceBango}`,
      );
    } catch (error) {
      console.warn(
        `[predict-queue] failed to defer prediction cache repair after warm miss category=${target.category} runYmd=${target.runYmd} keibajo=${target.keibajoCode} race=${target.raceBango}: ${String(error)}`,
      );
    }
  }
  return true;
};

const repairViewerPredictionCache = async (
  env: Env,
  target: ViewerPredictionCacheTarget,
): Promise<ViewerPredictionCacheRepairOutcome> => {
  const published = await publishPredictionKvForRace(env, {
    bustCacheApi: true,
    ...target,
  });
  if (published.status === "skipped-outside-window") return "outside-window";
  if (published.status !== "written" || !published.busted) return "retry";
  return (await warmViewerDisplayBestEffort(env, published, target, target.category))
    ? "complete"
    : "retry";
};

const enqueuePredictionCacheRepair = async (
  env: Env,
  target: ViewerPredictionCacheTarget,
  options?: PredictionCacheRepairEnqueueOptions,
): Promise<void> => {
  const message = {
    ...target,
    type: "prediction-cache-repair",
  } satisfies PredictionCacheRepairMessage;
  if (options === undefined) {
    await env.PREDICT_QUEUE.send(message);
    return;
  }
  await env.PREDICT_QUEUE.send(message, options);
};

const repairViewerPredictionCacheOrDefer = async (
  env: Env,
  target: ViewerPredictionCacheTarget,
): Promise<ViewerPredictionCacheRepairOutcome> => {
  try {
    return await repairViewerPredictionCache(env, target);
  } catch (error) {
    console.error(
      `[predict-queue] immediate prediction cache repair failed category=${target.category} runYmd=${target.runYmd} keibajo=${target.keibajoCode} race=${target.raceBango}:`,
      String(error),
    );
    return "retry";
  }
};

const ensureViewerPredictionCacheRepair = async (
  env: Env,
  message: Message<PredictQueueMessage>,
  target: ViewerPredictionCacheTarget,
): Promise<boolean> => {
  const repairTarget = toViewerPredictionCacheTarget(target);
  const outcome = await repairViewerPredictionCacheOrDefer(env, repairTarget);
  if (outcome !== "retry") return true;
  try {
    await enqueuePredictionCacheRepair(env, repairTarget, {
      delaySeconds: PREDICTION_CACHE_REPAIR_RETRY_DELAY_SECONDS,
    });
    return true;
  } catch (error) {
    console.error(
      `[predict-queue] failed to enqueue prediction cache repair category=${target.category} runYmd=${target.runYmd} keibajo=${target.keibajoCode} race=${target.raceBango}:`,
      String(error),
    );
    message.retry({ delaySeconds: PREDICTION_CACHE_REPAIR_RETRY_DELAY_SECONDS });
    return false;
  }
};

export const isPredictionCacheRepairQueueMessage = (
  message: Message<PredictQueueBody>,
): message is Message<PredictionCacheRepairMessage> =>
  "type" in message.body && message.body.type === "prediction-cache-repair";

const consumePredictionCacheRepair = async (
  message: Message<PredictionCacheRepairMessage>,
  env: Env,
): Promise<void> => {
  const outcome = await repairViewerPredictionCache(
    env,
    toViewerPredictionCacheTarget(message.body),
  );
  if (outcome === "retry") {
    if ((message.attempts ?? 1) >= PREDICTION_CACHE_REPAIR_MAX_ATTEMPTS) {
      console.warn(
        `[predict-queue] prediction cache repair exhausted category=${message.body.category} runYmd=${message.body.runYmd} keibajo=${message.body.keibajoCode} race=${message.body.raceBango} attempts=${message.attempts ?? 1}`,
      );
      message.ack();
      return;
    }
    message.retry({ delaySeconds: PREDICTION_CACHE_REPAIR_RETRY_DELAY_SECONDS });
    return;
  }
  message.ack();
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

const pickUpAndConfirmFocusedFullCache = async (
  params: FocusedFullCachePickupParams,
): Promise<boolean> => {
  const { env, message, role } = params;
  const { category, keibajoCode, raceBango, runYmd } = message;
  await pickUpFocusedFullCache({
    category,
    containerRole: role,
    debug: message.debug,
    env,
    keibajoCode,
    raceBango,
    runYmd,
  });
  const present = await isPerRaceFeatureCachePresent({
    category,
    env,
    keibajoCode,
    raceBango,
    runYmd,
  });
  if (!present) {
    console.warn(
      `Focused full cache still missing category=${category} runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango} -- keeping completion message for recovery`,
    );
  }
  return present;
};

// Queue attempts count delivery, not pipeline starts. A forced focused-full
// request can consume several attempts while waiting for day-base, running
// style, lane priority, or a Container slot. Preserve force until the Container
// status proves that the detached pipeline has started. Once /predict returns
// accepted, a redelivery observes running/success through the coordinator and
// never reaches the URL builder again.
const shouldForcePipelineStart = (message: Message<PredictQueueMessage>): boolean =>
  message.body.force === true &&
  (isFocusedSkipDedupMessage(message.body) ||
    message.attempts === undefined ||
    message.attempts <= 1);

const forcedCompletionNotBefore = (message: Message<PredictQueueMessage>): string | undefined => {
  if (message.body.force !== true) return undefined;
  if (message.body.forceRequestedAt !== undefined) return message.body.forceRequestedAt;
  return message.timestamp instanceof Date ? message.timestamp.toISOString() : undefined;
};

const resolveFocusedFullStatusTarget = async (
  body: FocusedFullSkipDedupMessage,
  env: Env,
): Promise<FocusedFullStatusTarget> => {
  const route = await resolveRaceContainerRoute({
    category: body.category,
    env,
    forceLegacy: body.forceLegacyContainer,
    focusedFull: true,
    keibajoCode: body.keibajoCode,
    raceBango: body.raceBango,
    runYmd: body.runYmd,
  });
  const doName = qualifyPredictionContainerDoName(
    resolvePredictDoName({
      category: body.category,
      env,
      keibajoCode: body.keibajoCode,
      raceBango: body.raceBango,
    }),
    route.role,
  );
  const workKey = buildFocusedFullWorkKey(body);
  const expectedRaceKey = `${body.category}:${body.runYmd}:${body.keibajoCode}:${body.raceBango}`;
  const doId = route.namespace.idFromName(doName);
  const stub = route.namespace.get(doId);
  return {
    doName,
    getContainerState: async () => (await stub.getState()).status,
    fetchStatus: async () => {
      const response = await stub.fetch(new Request(buildFocusedFullStatusUrl(body)));
      if (!response.ok) throw new Error(`Focused-full status returned ${response.status}`);
      return parseFocusedFullStatus(await response.json(), expectedRaceKey);
    },
    role: route.role,
    workKey,
  };
};

const enqueueFocusedFullCacheRepair = async (
  params: FocusedFullCacheRepairRequest,
): Promise<"enqueued" | "already-reserved" | "budget-exhausted"> => {
  const { body, env, message } = params;
  const reservationId = crypto.randomUUID();
  const reservation = await reserveFocusedFullRaceRepair({
    category: body.category,
    env,
    keibajoCode: body.keibajoCode,
    raceBango: body.raceBango,
    raceStartAtJst: body.raceStartAtJst,
    reservationId,
    runYmd: body.runYmd,
    staleAfterMs: FOCUSED_FULL_IN_FLIGHT_STALE_MS,
  });
  if (!reservation.proceed && reservation.state === "repair-budget-exhausted") {
    message.ack();
    console.error(
      `[predict-queue] focused-full repair budget exhausted reason=${params.reason} ${describePredictMessage(body)}`,
    );
    return "budget-exhausted";
  }
  if (!reservation.proceed && reservation.state !== "enqueued") {
    throw new Error(
      `Focused-full cache repair lane unavailable: ${reservation.state ?? "unknown"}`,
    );
  }
  if (reservation.proceed) {
    try {
      await env.PREDICT_QUEUE.send(
        {
          ...body,
          force: true,
          forceRequestedAt: new Date().toISOString(),
        },
        { delaySeconds: CONTAINER_SLOT_RETRY_DELAY_SECONDS },
      );
    } catch (error) {
      try {
        await cancelFocusedFullRaceRepair({
          category: body.category,
          env,
          keibajoCode: body.keibajoCode,
          raceBango: body.raceBango,
          reservationId,
          runYmd: body.runYmd,
        });
      } catch (releaseError) {
        console.error(
          "[predict-queue] failed to cancel focused-full cache-repair reservation:",
          String(releaseError),
        );
      }
      throw error;
    }
  }
  message.ack();
  console.warn(
    `[predict-queue] focused-full cache repair reason=${params.reason} outcome=${
      reservation.proceed ? "enqueued" : "already-reserved"
    } reservationId=${reservationId} ${describePredictMessage(body)}`,
  );
  return reservation.proceed ? "enqueued" : "already-reserved";
};

const recoverFocusedFullCache = async (params: FocusedFullCacheRepairRequest): Promise<void> => {
  // This path is an automatic repair, even though the replacement message
  // carries force=true so it can bypass a stale same-race completion marker.
  // Never let that internal force resurrect a historical Container after its
  // terminal watcher notices an absent in-process payload. Explicit admin
  // historical runs enter through the normal producer and remain available.
  if (isOldDateRunYmd(params.body.runYmd, new Date())) {
    params.message.ack();
    console.warn(
      `[predict-queue] skipping old focused-full cache repair reason=${params.reason} ${describePredictMessage(params.body)}`,
    );
    return;
  }
  try {
    await enqueueFocusedFullCacheRepair(params);
  } catch (error) {
    console.warn(
      `[predict-queue] focused-full ${params.reason} repair failed ${describePredictMessage(params.body)}:`,
      String(error),
    );
    params.message.retry({ delaySeconds: FOCUSED_FULL_RETRY_DELAY_SECONDS });
  }
};

const recoverMissingFocusedFullStatus = async (
  params: EnqueueFocusedFullCacheRepairParams,
): Promise<void> => {
  await recoverFocusedFullCache({
    body: params.body,
    env: params.env,
    message: params.message,
    reason: "missing-status",
  });
};

const recoverFocusedFullCacheAfterSuccess = async (
  params: CompleteFocusedFullFromStatusParams,
): Promise<void> => {
  await recoverFocusedFullCache({
    body: params.body,
    env: params.env,
    message: params.message,
    reason: "cache-missing-after-success",
  });
};

const recoverAlreadyCompleteFocusedFullCacheMiss = async (
  message: Message<PredictQueueMessage>,
  body: FocusedFullSkipDedupMessage,
  env: Env,
): Promise<void> => {
  const target = await resolveFocusedFullStatusTarget(body, env);
  try {
    const check: FocusedFullStatusCheck = { ...target, status: await target.fetchStatus() };
    if (check.status.status === "running") {
      await touchContainerSlot({
        doName: check.doName,
        env,
        staleAfterMs: FOCUSED_FULL_IN_FLIGHT_STALE_MS,
        workKey: check.workKey,
      });
      retryFocusedFullAlreadyInFlight(message);
      return;
    }
    if (check.status.status === "success") {
      const completionParams: CompleteFocusedFullFromStatusParams = {
        body,
        doName: check.doName,
        env,
        message,
        role: check.role,
        workKey: check.workKey,
      };
      if (!(await pickUpAndConfirmFocusedFullCache({ env, message: body, role: check.role }))) {
        await recoverFocusedFullCacheAfterSuccess(completionParams);
        return;
      }
      await completeFocusedFullAfterCacheConfirmed(completionParams);
      return;
    }
    if (check.status.status === "missing") {
      await recoverMissingFocusedFullStatus({ body, check, env, message });
      return;
    }
    await recoverFocusedFullStatus({
      body,
      doName: check.doName,
      env,
      error: check.status.error ?? `Focused-full detached pipeline failed: ${check.status.raceKey}`,
      message,
      role: check.role,
      workKey: check.workKey,
    });
  } catch (error) {
    console.warn(
      `[predict-queue] focused-full cache recovery failed ${describePredictMessage(body)}:`,
      String(error),
    );
    message.retry({ delaySeconds: FOCUSED_FULL_RETRY_DELAY_SECONDS });
  }
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
      notBefore: forcedCompletionNotBefore(message),
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
    const route = await resolveRaceContainerRoute({
      category,
      env,
      focusedFull: true,
      keibajoCode,
      raceBango,
      runYmd,
    });
    const doName = qualifyPredictionContainerDoName(
      resolvePredictDoName({ category, env, keibajoCode, raceBango }),
      route.role,
    );
    const workKey = buildFocusedFullWorkKey(message.body);
    const slot = await claimContainerSlot({
      allowSameOwner: true,
      category,
      doName,
      env,
      kind: FOCUSED_FULL_SLOT_KIND,
      staleAfterMs: FOCUSED_FULL_IN_FLIGHT_STALE_MS,
      workKey,
    });
    if (!slot.proceed) return false;
    // Neon confirms the race is done, but the detached pipeline that produced
    // it never had a live HTTP response to embed its R2 feat-cache payload
    // into (see focused-full-cache-pickup.ts's module docstring). This is the
    // one point in the redelivery flow that observes completion without ever
    // re-fetching the container, so it is also the only place a pickup can
    // happen. The R2 object is part of completion: acknowledging without it
    // leaves later rescore messages permanently deferred or forces a full
    // rebuild after the container's in-memory payload is gone.
    if (
      !(await pickUpAndConfirmFocusedFullCache({ env, message: message.body, role: route.role }))
    ) {
      await recoverAlreadyCompleteFocusedFullCacheMiss(message, message.body, env);
      return true;
    }
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
    await recordCompletedBestEffort(env, message.body);
    if (!(await ensureViewerPredictionCacheRepair(env, message, message.body))) {
      await tryHandOffTerminalContainerStop({ doName, env, role: route.role, workKey });
      return true;
    }
    if (!(await tryHandOffTerminalContainerStop({ doName, env, role: route.role, workKey }))) {
      message.retry({ delaySeconds: CONTAINER_SLOT_RETRY_DELAY_SECONDS });
      return true;
    }
    message.ack();
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

const recordPreweightGenerationStartedBestEffort = async (
  env: Env,
  message: PredictQueueMessage,
): Promise<void> => {
  try {
    await recordPreweightGenerationStarted(env, message, new Date());
  } catch (error) {
    console.error("[predict-queue] failed to record preweight generation start", String(error));
  }
};

const recordPreweightDisplayCompletedBestEffort = async (
  env: Env,
  message: PredictQueueMessage,
): Promise<void> => {
  try {
    await recordPreweightDisplayCompleted(env, message, new Date());
  } catch (error) {
    console.error("[predict-queue] failed to record preweight display completion", String(error));
  }
};

const releaseContainerSlotBestEffort = async (
  params: ReleaseContainerSlotBestEffortParams,
): Promise<void> => {
  try {
    await releaseContainerSlot(params);
  } catch (err) {
    console.error(
      `[predict-queue] failed to release container slot doName=${params.doName} kind=${params.kind}:`,
      String(err),
    );
  }
};

const clearContainerSlotBestEffort = async (
  env: Env,
  doName: string,
  workKey: string,
): Promise<void> => {
  try {
    await clearContainerSlot({ doName, env, workKey });
  } catch (err) {
    console.error(`[predict-queue] failed to clear container slot doName=${doName}:`, String(err));
  }
};

const claimContainerSlotOrRetry = async (
  params: ClaimContainerSlotOrRetryParams,
): Promise<boolean> => {
  const claim = await claimContainerSlot({
    ...(params.kind === FOCUSED_FULL_SLOT_KIND ? { allowSameOwner: true } : {}),
    category: params.category,
    doName: params.doName,
    env: params.env,
    kind: params.kind,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
    workKey: params.workKey,
  });
  if (claim.proceed) return true;
  console.warn(
    `[predict-queue] container slot ${claim.state ?? "capped"} doName=${params.doName} kind=${
      params.kind
    } ${describePredictMessage(params.message.body)} -- will retry without starting a container`,
  );
  if (params.kind === FOCUSED_FULL_SLOT_KIND && isFocusedSkipDedupMessage(params.message.body)) {
    await retryBusyFocusedFull(
      params.message,
      params.env,
      claim.state === CONTAINER_SLOT_CAPPED_STATE || claim.state === CONTAINER_SLOT_STOPPING_STATE,
    );
  } else if (params.kind === RESCORE_SLOT_KIND && isPerRaceRescore(params.message)) {
    retryDeferredRescore(params.message, "container-slot-unavailable");
  } else {
    params.message.retry({ delaySeconds: CONTAINER_SLOT_RETRY_DELAY_SECONDS });
  }
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

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const parseFocusedFullStatus = (
  value: unknown,
  expectedRaceKey: string,
): FocusedFullStatusPayload => {
  if (!isRecord(value)) throw new Error("Focused-full status response is not an object");
  const status = value.status;
  const raceKey = value.raceKey;
  const error = value.error;
  if (status !== "error" && status !== "missing" && status !== "running" && status !== "success")
    throw new Error("Focused-full status response has an invalid status");
  if (raceKey !== expectedRaceKey)
    throw new Error(`Focused-full status race key mismatch expected=${expectedRaceKey}`);
  if (error !== null && typeof error !== "string")
    throw new Error("Focused-full status response has an invalid error");
  const rawLastProgressAtMs = value.lastProgressAtMs;
  if (
    rawLastProgressAtMs !== undefined &&
    rawLastProgressAtMs !== null &&
    (typeof rawLastProgressAtMs !== "number" || !Number.isFinite(rawLastProgressAtMs))
  )
    throw new Error("Focused-full status response has an invalid lastProgressAtMs");
  return {
    error,
    lastProgressAtMs: rawLastProgressAtMs === undefined ? null : rawLastProgressAtMs,
    raceKey,
    status,
  };
};

const buildFocusedFullStatusUrl = (message: FocusedFullSkipDedupMessage): string => {
  const searchParams = new URLSearchParams({
    category: message.category,
    keibajoCode: message.keibajoCode,
    raceBango: message.raceBango,
    runDate: message.runYmd,
  });
  return `${PREDICT_HOST}${FOCUSED_FULL_STATUS_PATH}?${searchParams.toString()}`;
};

const repairDayBaseAfterContainerMiss = async (
  body: FocusedFullSkipDedupMessage,
  env: Env,
): Promise<void> => {
  try {
    await clearDayBaseRepairReservation({ category: body.category, env, runYmd: body.runYmd });
    await enqueueDayBaseRepairOnce({
      category: body.category,
      env,
      force: true,
      runYmd: body.runYmd,
    });
  } catch (error) {
    console.error(
      `[predict-queue] failed to schedule day-base repair after Container cache miss ${describePredictMessage(
        body,
      )}:`,
      String(error),
    );
  }
};

const recoverFocusedFullStatus = async (params: RecoverFocusedFullStatusParams): Promise<void> => {
  await persistRetryError(params.env, params.message, new Error(params.error));
  await completeFocusedFullRace({
    category: params.body.category,
    env: params.env,
    keibajoCode: params.body.keibajoCode,
    raceBango: params.body.raceBango,
    runYmd: params.body.runYmd,
    status: "error",
  });
  if (params.synchronousStop !== true) {
    await handOffTerminalContainerStop({
      doName: params.doName,
      env: params.env,
      role: params.role,
      workKey: params.workKey,
    });
  }
  if (params.role === "race-chain" && params.error.includes(DAY_BASE_REQUIRED_ERROR_CODE)) {
    await repairDayBaseAfterContainerMiss(params.body, params.env);
    params.message.retry({ delaySeconds: CONTAINER_SLOT_RETRY_DELAY_SECONDS });
    console.warn(
      `[predict-queue] detached race-chain cache HIT lost; retrying fail-closed ${describePredictMessage(
        params.message.body,
      )} error=${params.error} delaySeconds=${CONTAINER_SLOT_RETRY_DELAY_SECONDS}`,
    );
    return;
  }
  params.message.retry({ delaySeconds: CONTAINER_SLOT_RETRY_DELAY_SECONDS });
  console.warn(
    `[predict-queue] focused-full status error; cleanup handed off before retry ${describePredictMessage(
      params.message.body,
    )}; error=${params.error}`,
  );
};

const completeFocusedFullAfterCacheConfirmed = async (
  params: CompleteFocusedFullFromStatusParams,
): Promise<void> => {
  const { body, doName, env, message, role, workKey } = params;
  await completeFocusedFullRace({
    category: body.category,
    env,
    keibajoCode: body.keibajoCode,
    raceBango: body.raceBango,
    runYmd: body.runYmd,
    status: "success",
  });
  await recordCompletedBestEffort(env, body);
  if (!(await ensureViewerPredictionCacheRepair(env, message, body))) {
    if (params.synchronousStop !== true) {
      await tryHandOffTerminalContainerStop({ doName, env, role, workKey });
    }
    return;
  }
  if (params.synchronousStop === true) {
    message.ack();
    return;
  }
  if (!(await tryHandOffTerminalContainerStop({ doName, env, role, workKey }))) {
    message.retry({ delaySeconds: CONTAINER_SLOT_RETRY_DELAY_SECONDS });
    return;
  }
  message.ack();
};

const completeFocusedFullFromStatus = async (
  params: CompleteFocusedFullFromStatusParams,
): Promise<void> => {
  const cachePresent = await pickUpAndConfirmFocusedFullCache({
    env: params.env,
    message: params.body,
    role: params.role,
  });
  if (
    !cachePresent &&
    !(await pickUpAndConfirmFocusedFullCache({
      env: params.env,
      message: params.body,
      role: params.role,
    }))
  ) {
    if (params.synchronousStop === true) {
      throw new Error("FOCUSED_FULL_WATCH_CACHE_MISSING_AFTER_SUCCESS");
    }
    await recoverFocusedFullCacheAfterSuccess(params);
    return;
  }
  await completeFocusedFullAfterCacheConfirmed(params);
};

const pollFocusedFullStatus = async (
  message: Message<PredictQueueMessage>,
  body: FocusedFullSkipDedupMessage,
  env: Env,
): Promise<boolean> => {
  const target = await resolveFocusedFullStatusTarget(body, env);
  try {
    const { doName, role, workKey } = target;
    const status = await target.fetchStatus();
    if (status.status === "running") {
      if (
        status.lastProgressAtMs !== null &&
        Date.now() - status.lastProgressAtMs > FOCUSED_FULL_PROGRESS_STALE_MS
      ) {
        await recoverFocusedFullStatus({
          body,
          doName,
          env,
          error: "Focused-full detached pipeline heartbeat stale",
          message,
          role,
          workKey,
        });
        return false;
      }
      await touchContainerSlot({
        doName,
        env,
        staleAfterMs: FOCUSED_FULL_IN_FLIGHT_STALE_MS,
        workKey,
      });
      retryFocusedFullAlreadyInFlight(message);
      return false;
    }
    if (status.status === "success") {
      await completeFocusedFullFromStatus({
        body,
        doName,
        env,
        message,
        role,
        workKey,
      });
      return false;
    }
    if (status.status === "missing") {
      // A delivery may acquire its coordinator lane before a capped/stopping
      // Container slot defers it. No /predict request happened in that case,
      // so a missing runtime status must continue to the downstream slot claim
      // instead of converting a never-started request into a repair loop.
      // Running/success above prove an accepted start and prevent duplicate
      // execution on redelivery. The Neon-complete/R2-missing path is handled
      // separately by recoverAlreadyCompleteFocusedFullCacheMiss.
      return true;
    }
    const error = status.error ?? `Focused-full detached pipeline failed: ${status.raceKey}`;
    await recoverFocusedFullStatus({
      body,
      doName,
      env,
      error,
      message,
      role,
      workKey,
    });
    return false;
  } catch (error) {
    const statusQueryError = String(error);
    console.warn(
      `[predict-queue] focused-full status query failed ${describePredictMessage(message.body)}:`,
      statusQueryError,
    );
    const containerState = await target.getContainerState().catch((stateError: unknown) => {
      console.warn(
        `[predict-queue] focused-full Container state query failed ${describePredictMessage(
          message.body,
        )}:`,
        String(stateError),
      );
      return null;
    });
    if (containerState === "stopped" || containerState === "stopped_with_code") {
      await recoverFocusedFullStatus({
        body,
        doName: target.doName,
        env,
        error: `Focused-full Container stopped before terminal status: ${statusQueryError}`,
        message,
        role: target.role,
        workKey: target.workKey,
      });
      return false;
    }
    if (containerState !== null) {
      console.warn(
        `[predict-queue] focused-full status retry while Container remains ${containerState} ${describePredictMessage(
          message.body,
        )}`,
      );
    }
    retryFocusedFullAlreadyInFlight(message);
    return false;
  }
};

const claimFocusedFullOrRetry = async (
  message: Message<PredictQueueMessage>,
  body: FocusedFullSkipDedupMessage,
  env: Env,
): Promise<boolean> => {
  const { category, keibajoCode, raceBango, runYmd } = body;
  const claim = await claimFocusedFullRace({
    category,
    doName: resolvePredictDoName({ category, env, keibajoCode, raceBango }),
    env,
    force: shouldForcePipelineStart(message),
    keibajoCode,
    raceStartAtJst: body.raceStartAtJst,
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
  if (claim.proceed) {
    if (claim.state === "resumed") {
      return pollFocusedFullStatus(message, body, env);
    }
    if (claim.state === "stale") {
      await clearContainerSlotBestEffort(
        env,
        resolvePredictDoName({ category, env, keibajoCode, raceBango }),
        buildFocusedFullWorkKey(body),
      );
    }
    return true;
  }
  if (claim.state === "queued") {
    await retryBusyFocusedFull(message, env, true);
    return false;
  }
  if (claim.state === "started") {
    return pollFocusedFullStatus(message, body, env);
  }
  // A status probe is itself a Container request and starts an inactive
  // instance. Neon completion has already been checked before this claim, so
  // unresolved work stays on the Queue until the coordinator's 31-minute
  // stale deadline permits an intentional restart.
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
    `busyRequeueCount=${body.busyRequeueCount ?? LEGACY_BUSY_REQUEUE_COUNT_ZERO}`,
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

// Queue attempts start at 1. Keep the first retry at the base delay, then grow
// linearly while retaining the original message id and bounded retry budget.
const computeRetryDelaySeconds = (attempts: number): number =>
  Math.min(
    BUSY_RETRY_DELAY_BASE_SECONDS + BUSY_RETRY_DELAY_STEP_SECONDS * Math.max(attempts - 1, 0),
    BUSY_RETRY_DELAY_MAX_SECONDS,
  );

const computeRescoreRetryDelaySeconds = (
  message: PerRaceRescoreMessage,
  attempts: number,
  nowMs: number,
): number => {
  const baseDelaySeconds = computeRetryDelaySeconds(attempts);
  const postMs = Date.parse(message.raceStartAtJst ?? "");
  if (!Number.isFinite(postMs)) return baseDelaySeconds;
  const secondsToPost = Math.floor((postMs - nowMs) / 1000);
  if (secondsToPost <= MIN_RETRY_DELAY_SECONDS) return MIN_RETRY_DELAY_SECONDS;
  const deadlineDelay = Math.max(
    MIN_RETRY_DELAY_SECONDS,
    Math.min(baseDelaySeconds, secondsToPost - MIN_RETRY_DELAY_SECONDS),
  );
  return secondsToPost <= RESCORE_NEAR_POST_THRESHOLD_SECONDS
    ? Math.min(deadlineDelay, RESCORE_NEAR_POST_RETRY_MAX_SECONDS)
    : deadlineDelay;
};

const retryAfterFailure = async (
  message: Message<PredictQueueMessage>,
  env: Env,
  err: unknown,
): Promise<void> => {
  await persistRetryError(env, message, err);
  const delaySeconds = isPerRaceRescore(message)
    ? computeRescoreRetryDelaySeconds(message.body, message.attempts, Date.now())
    : computeRetryDelaySeconds(message.attempts);
  message.retry({ delaySeconds });
};

const deferFocusedFullUntilDayBaseReady = async (
  message: Message<PredictQueueMessage>,
  env: Env,
): Promise<boolean> => {
  if (!isFocusedSkipDedupMessage(message.body)) return false;
  const { category, keibajoCode, raceBango, runYmd } = message.body;
  try {
    const readiness = await getFocusedFullDayBaseReadiness({ category, env, runYmd });
    if (readiness.ready) {
      let foundation = await getDayBaseRaceFoundationReadiness({
        category,
        env,
        raceNumber: raceBango,
        runYmd,
        venueCode: keibajoCode,
      });
      if (!foundation.ready) {
        const materialized = await materializeDayBasePerRaceCache({
          category,
          env,
          force: true,
          runYmd,
        });
        if (materialized.status !== "materialized") {
          throw new Error(`per-race foundation warm failed: ${materialized.reason}`);
        }
        foundation = await getDayBaseRaceFoundationReadiness({
          category,
          env,
          raceNumber: raceBango,
          runYmd,
          venueCode: keibajoCode,
        });
      }
      if (!foundation.ready) {
        const delaySeconds = computeRetryDelaySeconds(message.attempts);
        message.retry({ delaySeconds });
        console.warn(
          `[predict-queue] focused-full foundation deferred before claim ${describePredictMessage(
            message.body,
          )} reason=${foundation.reason} attempts=${message.attempts} delaySeconds=${delaySeconds}`,
        );
        return true;
      }
      console.log(
        `[predict-queue] feature foundation HIT before Container category=${category} runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango} source=worker-r2-warm`,
      );
      try {
        await clearDayBaseRepairReservation({ category, env, runYmd });
      } catch (error) {
        console.error(
          `[predict-queue] failed to clear day-base repair reservation category=${category} runYmd=${runYmd}:`,
          String(error),
        );
      }
      return false;
    }
    const repair = await enqueueDayBaseRepairOnce({
      category,
      env,
      ...(message.body.force === true ? { force: true } : {}),
      runYmd,
    });
    const delaySeconds = computeRetryDelaySeconds(message.attempts);
    message.retry({ delaySeconds });
    console.warn(
      `[predict-queue] focused-full day-base deferred before claim ${describePredictMessage(
        message.body,
      )} reason=${readiness.reason} repair=${repair} attempts=${message.attempts} delaySeconds=${delaySeconds}`,
    );
    return true;
  } catch (error) {
    console.error(
      `[predict-queue] focused-full day-base readiness failed before claim ${describePredictMessage(
        message.body,
      )}:`,
      String(error),
    );
    await retryAfterFailure(message, env, error);
    return true;
  }
};

// This focused-full message's race never started: the container's single
// per-process pipeline slot for this category was busy with a DIFFERENT race.
const retryBusyFocusedFull = async (
  message: Message<PredictQueueMessage>,
  env: Env,
  preserveLanePriority = false,
): Promise<void> => {
  const { category, keibajoCode, raceBango, runYmd } = message.body;
  const suffix = raceScopeSuffix(keibajoCode, raceBango);
  // A coordinator-queued race remains a waiter in the lane. Completing it as
  // error removes it from FIFO and makes the replacement join at the tail.
  // Container-slot/Python busy is different: that race is the active owner
  // and must yield so a real owner mismatch cannot poison the lane.
  if (!preserveLanePriority && isFocusedSkipDedupMessage(message.body)) {
    await completeFocusedFullRace({
      category,
      env,
      keibajoCode: message.body.keibajoCode,
      raceBango: message.body.raceBango,
      runYmd,
      status: "error",
    });
  }
  const delaySeconds = computeRetryDelaySeconds(message.attempts);
  message.retry({ delaySeconds });
  debugLog(
    message.body,
    `Focused full slot busy, retrying category=${category} runYmd=${runYmd}${suffix} attempts=${message.attempts} delaySeconds=${delaySeconds}`,
  );
};

const retryDeferredRescore = (message: Message<PerRaceRescoreMessage>, reason: string): void => {
  const { category, keibajoCode, raceBango, runYmd } = message.body;
  const suffix = raceScopeSuffix(keibajoCode, raceBango);
  const delaySeconds = computeRescoreRetryDelaySeconds(message.body, message.attempts, Date.now());
  message.retry({ delaySeconds });
  console.warn(
    `Rescore deferred category=${category} runYmd=${runYmd}${suffix} reason=${reason} attempts=${message.attempts} delaySeconds=${delaySeconds}`,
  );
};

const releaseRescoreSlot = (
  message: Message<PerRaceRescoreMessage>,
  env: Env,
  doName: string,
): Promise<void> =>
  releaseContainerSlotBestEffort({
    doName,
    env,
    kind: RESCORE_SLOT_KIND,
    workKey: buildPredictWorkKey(message.body),
  });

const claimRescoreExecutionOrFinish = async (
  message: Message<PerRaceRescoreMessage>,
  env: Env,
  doName: string,
): Promise<boolean> => {
  const { category, keibajoCode, raceBango, runYmd } = message.body;
  try {
    const claim = await claimRescoreExecution({
      category,
      env,
      executionId: message.id,
      keibajoCode,
      raceBango,
      runYmd,
      staleAfterMs: RESCORE_EXECUTION_STALE_MS,
      weightSnapshotCount: message.body.weightSnapshotCount,
      weightSnapshotFetchedAt: message.body.weightSnapshotFetchedAt,
      weightSnapshotHash: message.body.weightSnapshotHash,
    });
    if (claim.proceed) return true;
    if (claim.state === "success") {
      await handOffTerminalContainerStop({
        doName,
        env,
        role: "legacy",
        workKey: buildPredictWorkKey(message.body),
      });
      message.ack();
      return false;
    }
    retryDeferredRescore(message, `execution-${claim.state ?? "claimed"}`);
    return false;
  } catch (error) {
    await retryAfterFailure(message, env, error);
    return false;
  }
};

const claimWorkerRescoreExecutionOrFinish = async (
  message: Message<PerRaceRescoreMessage>,
  env: Env,
): Promise<boolean> => {
  const { category, keibajoCode, raceBango, runYmd } = message.body;
  try {
    const claim = await claimRescoreExecution({
      category,
      env,
      executionId: message.id,
      keibajoCode,
      raceBango,
      runYmd,
      staleAfterMs: RESCORE_EXECUTION_STALE_MS,
      weightSnapshotCount: message.body.weightSnapshotCount,
      weightSnapshotFetchedAt: message.body.weightSnapshotFetchedAt,
      weightSnapshotHash: message.body.weightSnapshotHash,
    });
    if (claim.proceed) return true;
    if (claim.state === "success") {
      message.ack();
      return false;
    }
    retryDeferredRescore(message, `execution-${claim.state ?? "claimed"}`);
    return false;
  } catch (error) {
    await retryAfterFailure(message, env, error);
    return false;
  }
};

const completeRescoreExecution = (
  message: Message<PerRaceRescoreMessage>,
  env: Env,
  status: "error" | "success",
): Promise<void> =>
  completeRescoreRace({
    category: message.body.category,
    env,
    executionId: message.id,
    keibajoCode: message.body.keibajoCode,
    raceBango: message.body.raceBango,
    runYmd: message.body.runYmd,
    status,
    weightSnapshotCount: message.body.weightSnapshotCount,
    weightSnapshotFetchedAt: message.body.weightSnapshotFetchedAt,
    weightSnapshotHash: message.body.weightSnapshotHash,
  });

const completeRescoreExecutionErrorBestEffort = async (
  message: Message<PerRaceRescoreMessage>,
  env: Env,
): Promise<void> => {
  try {
    await completeRescoreExecution(message, env, "error");
  } catch (error) {
    console.error(
      `[predict-queue] failed to complete rescore execution status=error ${describePredictMessage(
        message.body,
      )}:`,
      String(error),
    );
  }
};

const finishExpiredRescore = async (input: FinishExpiredRescoreInput): Promise<void> => {
  console.warn(
    `Dropping expired rescore ${describePredictMessage(input.message.body)} stage=${input.stage} raceStartAtJst=${input.message.body.raceStartAtJst}`,
  );
  await completeRescoreExecutionErrorBestEffort(input.message, input.env);
  input.message.ack();
};

const deferRescoreUntilInitialPrediction = async (
  message: Message<PerRaceRescoreMessage>,
  env: Env,
): Promise<boolean> => {
  const { category, keibajoCode, raceBango, runYmd } = message.body;
  try {
    const ready = await isPerRaceRescoreReady({
      category,
      env,
      keibajoCode,
      raceBango,
      runYmd,
    });
    if (ready) return false;
    retryDeferredRescore(message, "initial-prediction-or-cache-incomplete");
    return true;
  } catch (err) {
    console.error(
      `Rescore readiness check failed category=${category} runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango}:`,
      String(err),
    );
    await retryAfterFailure(message, env, err);
    return true;
  }
};

// Dispatches focused per-race full statuses ("accepted" / "already-complete" /
// "busy") to their handling and returns whether the status was one of these
// (in which case processMessage's caller must stop, having already
// acked/retried). Returns false for any other status (e.g. "success" or
// "error") so the caller falls through to the shared success/error handling.
const handleFocusedFullStatus = async (params: HandleFocusedFullStatusParams): Promise<boolean> => {
  const { doName, env, message, role, slotHold, status, watchId, workKey } = params;
  const { category, keibajoCode, raceBango, runYmd } = message.body;
  const suffix = raceScopeSuffix(keibajoCode, raceBango);
  if (status === FOCUSED_FULL_ACCEPTED_STATUS) {
    debugLog(
      message.body,
      `Focused full accepted, still in progress category=${category} runYmd=${runYmd}${suffix} -- will re-check on redelivery`,
    );
    slotHold.keep = FOCUSED_FULL_ACCEPTED_KEEP_SLOT;
    if (watchId !== undefined && watchId.length > 0) {
      message.ack();
      console.log(
        `[predict-queue] focused-full durable watch accepted watchId=${watchId} ${describePredictMessage(message.body)}`,
      );
      return true;
    }
    message.retry({ delaySeconds: FOCUSED_FULL_RETRY_DELAY_SECONDS });
    return true;
  }
  if (status === FOCUSED_FULL_ALREADY_COMPLETE_STATUS) {
    debugLog(
      message.body,
      `Focused full already complete (container) category=${category} runYmd=${runYmd}${suffix}`,
    );
    if (isFocusedSkipDedupMessage(message.body)) {
      const cachePresent = await pickUpAndConfirmFocusedFullCache({
        env,
        message: message.body,
        role,
      });
      if (
        !cachePresent &&
        !(await pickUpAndConfirmFocusedFullCache({ env, message: message.body, role }))
      ) {
        slotHold.keep = true;
        await recoverFocusedFullCacheAfterSuccess({
          body: message.body,
          doName,
          env,
          message,
          role,
          workKey,
        });
        return true;
      }
      await completeFocusedFullRace({
        category,
        env,
        keibajoCode: message.body.keibajoCode,
        raceBango: message.body.raceBango,
        runYmd,
        status: "success",
      });
    }
    await recordCompletedBestEffort(env, message.body);
    if (isFocusedSkipDedupMessage(message.body)) {
      if (!(await ensureViewerPredictionCacheRepair(env, message, message.body))) {
        slotHold.keep = true;
        await tryHandOffTerminalContainerStop({ doName, env, role, workKey });
        return true;
      }
      slotHold.keep = true;
      if (!(await tryHandOffTerminalContainerStop({ doName, env, role, workKey }))) {
        message.retry({ delaySeconds: CONTAINER_SLOT_RETRY_DELAY_SECONDS });
        return true;
      }
    }
    message.ack();
    return true;
  }
  if (status === FOCUSED_FULL_BUSY_STATUS) {
    await retryBusyFocusedFull(message, env);
    return true;
  }
  return false;
};

const retryDayBaseRequiredFailClosed = async (
  message: Message<PredictQueueMessage>,
  env: Env,
  role: PredictionContainerRole,
  result: PredictResultLine,
): Promise<boolean> => {
  if (role !== "race-chain" || !isFocusedSkipDedupMessage(message.body)) return false;
  if (result.status !== "error" || !result.error?.includes(DAY_BASE_REQUIRED_ERROR_CODE))
    return false;
  await completeFocusedFullRace({
    category: message.body.category,
    env,
    keibajoCode: message.body.keibajoCode,
    raceBango: message.body.raceBango,
    runYmd: message.body.runYmd,
    status: "error",
  });
  await repairDayBaseAfterContainerMiss(message.body, env);
  message.retry({ delaySeconds: CONTAINER_SLOT_RETRY_DELAY_SECONDS });
  console.warn(
    `[predict-queue] race-chain cache HIT lost; retrying fail-closed ${describePredictMessage(
      message.body,
    )} error=${result.error} delaySeconds=${CONTAINER_SLOT_RETRY_DELAY_SECONDS}`,
  );
  return true;
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

const isInactiveContainerDoError = (error: unknown): boolean =>
  error instanceof Error && error.message.startsWith(INACTIVE_CONTAINER_DO_ERROR_PREFIX);

const fetchPerRaceRescoreWithReconnect = async (
  env: Env,
  doId: DurableObjectId,
  predictUrl: string,
  message: PerRaceRescoreMessage,
  predictDoName: string,
): Promise<Response> => {
  try {
    return await env.FINISH_POSITION_PREDICT_CONTAINER.get(doId).fetch(new Request(predictUrl));
  } catch (error) {
    if (!isInactiveContainerDoError(error)) throw error;
    console.warn(
      `[predict-queue] container DO became inactive; reconnecting once ${describePredictMessage(
        message,
      )} doName=${predictDoName}`,
    );
    return env.FINISH_POSITION_PREDICT_CONTAINER.get(doId).fetch(new Request(predictUrl));
  }
};

// Container per-race rescore: held /predict on a per-race DO
// runs the Python ensemble re-score for one race. No per-category run state is
// touched (completeRun is not called) so concurrent full/per-category runs are
// unaffected. A successful NDJSON result (status omitted or success) acks whether
// racesPredicted is > 0 or 0 (cache_miss). Fetch / stream / DO errors, and final
// result status:error, retry via the queue's DLQ machinery. After a
// successful ack the viewer Cache API is warmed for the same race so the
// event-driven horse-weight trigger surfaces fresh predictions on the race
// detail page without waiting for cache TTL. KV write-through and the semantic
// success commit are awaited before terminal cleanup. Stop/warm failures after
// that commit must not make Queue redelivery run scoring a second time.
const processContainerPerRaceRescore = async (
  message: Message<PerRaceRescoreMessage>,
  env: Env,
): Promise<void> => {
  const {
    category,
    daysAhead,
    debug,
    keibajoCode,
    raceBango,
    runYmd,
    weightSnapshotCount,
    weightSnapshotFetchedAt,
    weightSnapshotHash,
  } = message.body;
  const raceStartAtJst = message.body.raceStartAtJst;
  if (raceStartAtJst === undefined) {
    message.ack();
    return;
  }
  if (
    weightSnapshotCount === undefined ||
    weightSnapshotFetchedAt === undefined ||
    weightSnapshotHash === undefined
  ) {
    const error = new Error("Horse weight snapshot generation is missing");
    console.error(
      `Container per-race rescore rejected before dispatch category=${category} runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango}:`,
      error.message,
    );
    await retryAfterFailure(message, env, error);
    return;
  }
  const startedAt = Date.now();
  if (await deferRescoreUntilInitialPrediction(message, env)) return;
  const cardMaxRaceBango = await resolveCardMaxRaceBangoForKochi({ env, keibajoCode, runYmd });
  const predictDoName = resolvePredictDoName({
    category,
    env,
    keibajoCode,
    raceBango,
  });
  if (!(await claimRescoreExecutionOrFinish(message, env, predictDoName))) return;
  if (
    !(await claimContainerSlotOrRetry({
      category,
      doName: predictDoName,
      env,
      kind: RESCORE_SLOT_KIND,
      message,
      workKey: buildPredictWorkKey(message.body),
    }))
  ) {
    await completeRescoreExecutionErrorBestEffort(message, env);
    return;
  }
  const basePredictUrl = buildPerRaceRescoreUrl({
    cardMaxRaceBango,
    category,
    daysAhead,
    debug,
    keibajoCode,
    raceBango,
    raceStartAtJst,
    runYmd,
    activeHorseNumbers: message.body.activeHorseNumbers,
    entrySnapshotFetchedAt: message.body.entrySnapshotFetchedAt,
    entrySnapshotHash: message.body.entrySnapshotHash,
    excludedHorseNumbers: message.body.excludedHorseNumbers,
    weightSnapshotCount,
    weightSnapshotFetchedAt,
    weightSnapshotHash,
  });
  const doId = env.FINISH_POSITION_PREDICT_CONTAINER.idFromName(predictDoName);
  const lifecycle: ContainerRequestLifecycle = { started: false, terminal: false };
  const cleanupHandedOff = { value: false };
  const deadlineExpired = { value: false };
  try {
    const [commitResult] = await Promise.allSettled([
      (async () => {
        if (
          !isBeforeRaceStartDeadline({
            nowMs: Date.now(),
            raceStartAtJst,
          })
        ) {
          deadlineExpired.value = true;
          await finishExpiredRescore({ env, message, stage: "container-start" });
          return;
        }
        const attestation = await createRescoreAttestation({
          category,
          env,
          keibajoCode,
          raceBango,
          runYmd,
        });
        console.log(
          `[predict-queue] rescore feature HIT before Container category=${category} runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango} entries=${attestation.entryCount}`,
        );
        const predictUrl = addRescoreAttestationToUrl(basePredictUrl, attestation);
        debugLog(
          message.body,
          `[predict-queue] container-fetch start ${describePredictMessage(
            message.body,
          )} doName=${predictDoName} url=${predictUrl}`,
        );
        lifecycle.started = true;
        const response = await fetchPerRaceRescoreWithReconnect(
          env,
          doId,
          predictUrl,
          message.body,
          predictDoName,
        );
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
        if (
          !isBeforeRaceStartDeadline({
            nowMs: Date.now(),
            raceStartAtJst,
          })
        ) {
          deadlineExpired.value = true;
          await finishExpiredRescore({ env, message, stage: "container-score-complete" });
          return;
        }
        console.log(
          `Rescore container category=${category} runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango} races=${result.racesPredicted} durationMs=${
            Date.now() - startedAt
          }`,
        );
        const published = await publishAndWarmRescoreDisplay(env, {
          category,
          keibajoCode,
          raceBango,
          raceStartAtJst,
          runYmd,
        });
        if (!published) {
          deadlineExpired.value = true;
          await finishExpiredRescore({ env, message, stage: "container-kv-publish" });
          return;
        }
        await completeRescoreExecution(message, env, "success");
      })(),
    ]);
    lifecycle.terminal = true;
    if (commitResult.status === "rejected") {
      console.error(
        `Container per-race rescore failed category=${category} runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango} durationMs=${
          Date.now() - startedAt
        }:`,
        String(commitResult.reason),
      );
      await completeRescoreExecutionErrorBestEffort(message, env);
      await retryAfterFailure(message, env, commitResult.reason);
      return;
    }
    if (deadlineExpired.value) {
      if (lifecycle.started) {
        await handOffTerminalContainerStop({
          doName: predictDoName,
          env,
          role: "legacy",
          workKey: buildPredictWorkKey(message.body),
        });
        cleanupHandedOff.value = true;
      }
      return;
    }
    await handOffTerminalContainerStop({
      doName: predictDoName,
      env,
      role: "legacy",
      workKey: buildPredictWorkKey(message.body),
    });
    cleanupHandedOff.value = true;
    message.ack();
  } finally {
    if (lifecycle.started && lifecycle.terminal && !cleanupHandedOff.value) {
      cleanupHandedOff.value = true;
      await handOffTerminalContainerStop({
        doName: predictDoName,
        env,
        role: "legacy",
        workKey: buildPredictWorkKey(message.body),
      });
    } else if (!lifecycle.started) {
      await releaseRescoreSlot(message, env, predictDoName);
    }
  }
};

const processWorkerJraPerRaceRescore = async (
  message: Message<PerRaceRescoreMessage>,
  env: Env,
): Promise<void> => {
  const { category, keibajoCode, raceBango, runYmd } = message.body;
  const startedAt = Date.now();
  if (await deferRescoreUntilInitialPrediction(message, env)) return;
  if (!(await claimWorkerRescoreExecutionOrFinish(message, env))) return;
  try {
    if (
      !isBeforeRaceStartDeadline({
        nowMs: Date.now(),
        raceStartAtJst: message.body.raceStartAtJst,
      })
    ) {
      await finishExpiredRescore({ env, message, stage: "worker-start" });
      return;
    }
    const result = await rescoreJraRace({ env, fetchImpl: fetch, message: message.body });
    if (result.status !== "ok") {
      throw new Error(`Worker rescore unavailable: ${result.status}`);
    }
    if (
      !isBeforeRaceStartDeadline({
        nowMs: Date.now(),
        raceStartAtJst: message.body.raceStartAtJst,
      })
    ) {
      await finishExpiredRescore({ env, message, stage: "worker-score-complete" });
      return;
    }
    const published = await publishAndWarmRescoreDisplay(env, {
      category,
      keibajoCode,
      raceBango,
      raceStartAtJst: message.body.raceStartAtJst,
      runYmd,
    });
    if (!published) {
      await finishExpiredRescore({ env, message, stage: "worker-kv-publish" });
      return;
    }
    await completeRescoreExecution(message, env, "success");
    console.log(
      `Rescore Worker category=${category} runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango} races=${result.racesPredicted} predictions=${result.predictionCount} model=${result.modelVersion} durationMs=${Date.now() - startedAt}`,
    );
    message.ack();
  } catch (error) {
    if (error instanceof RaceDeadlineExceededError) {
      await finishExpiredRescore({ env, message, stage: "worker-neon-publish" });
      return;
    }
    console.warn(
      `Worker per-race rescore falling back to Container category=${category} runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango} durationMs=${Date.now() - startedAt}:`,
      String(error),
    );
    await completeRescoreExecutionErrorBestEffort(message, env);
    await processContainerPerRaceRescore(message, env);
  }
};

// Per-race rescore dispatch: supported categories route to the container held
// /predict, unknown categories are skipped + acked.
const processPerRaceRescore = (
  message: Message<PerRaceRescoreMessage>,
  env: Env,
): Promise<void> => {
  const { category, runYmd } = message.body;
  if (
    !isBeforeRaceStartDeadline({
      nowMs: Date.now(),
      raceStartAtJst: message.body.raceStartAtJst,
    })
  ) {
    console.warn(
      `Dropping post-time rescore category=${category} runYmd=${runYmd}${raceScopeSuffix(
        message.body.keibajoCode,
        message.body.raceBango,
      )} raceStartAtJst=${message.body.raceStartAtJst}`,
    );
    message.ack();
    return Promise.resolve();
  }
  if (category === "jra" && env.JRA_WORKER_RESCORE_ENABLED === ENABLED_FLAG)
    return processWorkerJraPerRaceRescore(message, env);
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
      await clearDayBaseRepairReservation({ category, env, runYmd });
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

const cleanupPastDayBaseWork = async (params: PastDayBaseWorkParams): Promise<void> => {
  await Promise.all([
    clearDayBaseRepairReservation(params).catch((error) => {
      console.error(
        `[predict-queue] old day-base repair reservation cleanup failed category=${params.category} runYmd=${params.runYmd}:`,
        String(error),
      );
    }),
    cleanupDayBaseWork(params).catch((error) => {
      console.error(
        `[predict-queue] old day-base container cleanup failed category=${params.category} runYmd=${params.runYmd}:`,
        String(error),
      );
    }),
  ]);
};

const handlePastDayBaseSkip = async (
  message: Message<DayBasePrewarmMessage>,
  env: Env,
): Promise<void> => {
  await cleanupPastDayBaseWork({
    category: message.body.category,
    env,
    ...dayBaseGenerationFields(message.body.generationId),
    runYmd: message.body.runYmd,
  });
  message.ack();
  console.warn(
    `[predict-queue] skipping old day-base message type=${message.body.type} category=${message.body.category} runYmd=${message.body.runYmd}`,
  );
};

const handleInvalidPerRaceScopeSkip = (
  message: Message<PredictQueueMessage>,
  reason: string,
): void => {
  console.warn(
    `Skipping invalid predict message ${describePredictMessage(message.body)}: ${reason}`,
  );
  message.ack();
};

const processMessage = async (message: Message<PredictQueueMessage>, env: Env): Promise<void> => {
  const raceTarget = {
    keibajoCode: message.body.keibajoCode,
    raceBango: message.body.raceBango,
  };
  if (!hasValidPerRaceScope(raceTarget)) {
    const reason = hasRequiredPerRaceScope(raceTarget)
      ? PER_RACE_SCOPE_INVALID_ERROR
      : PER_RACE_SCOPE_REQUIRED_ERROR;
    handleInvalidPerRaceScopeSkip(message, reason);
    return;
  }
  await recordConsumedBestEffort(env, message.body);
  if (isOldDateRunYmd(message.body.runYmd, new Date())) return handleOldDateSkip(message, env);
  debugLog(message.body, `[predict-queue] received ${describePredictMessage(message.body)}`);
  if (isPerRaceRescore(message)) return processPerRaceRescore(message, env);
  const { category, runYmd, daysAhead, mode, keibajoCode, raceBango, skipDedup } = message.body;
  const startedAt = Date.now();
  const isFocusedSkipDedup = isFocusedSkipDedupMessage(message.body);
  const workKey = isFocusedSkipDedup
    ? buildFocusedFullWorkKey(message.body)
    : buildPredictWorkKey(message.body);
  const shouldCompleteCategoryRun = !isFocusedSkipDedup;
  const cardMaxRaceBango = await resolveCardMaxRaceBangoForKochi({ env, keibajoCode, runYmd });
  if (await deferFocusedFullUntilDayBaseReady(message, env)) return;
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
  const marketSignalHook = isFocusedSkipDedupMessage(message.body)
    ? await prepareMarketSignalFoundationBestEffort({
        category,
        env: {
          FEATURES_CACHE: env.FEATURES_CACHE,
          WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED: env.WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED,
        },
        fetchImpl: fetch,
        keibajoCode: message.body.keibajoCode,
        raceBango: message.body.raceBango,
        runYmd,
      })
    : undefined;
  const containerRoute = await resolveRaceContainerRoute({
    category,
    env,
    forceLegacy: message.body.forceLegacyContainer,
    focusedFull: isFocusedSkipDedup,
    ...(isFocusedSkipDedupMessage(message.body)
      ? { keibajoCode: message.body.keibajoCode, raceBango: message.body.raceBango }
      : {}),
    runYmd,
  });
  const predictDoName = qualifyPredictionContainerDoName(
    resolvePredictDoName({ category, env, keibajoCode, raceBango }),
    containerRoute.role,
  );
  if (
    !(await claimContainerSlotOrRetry({
      category,
      doName: predictDoName,
      env,
      kind: FOCUSED_FULL_SLOT_KIND,
      message,
      workKey,
    }))
  ) {
    return;
  }
  const basePredictUrl = buildPredictUrl({
    cardMaxRaceBango,
    category,
    daysAhead,
    // Python debug mode keeps focused-full /predict attached until every
    // feature layer finishes. That can exceed the Worker/DO connection
    // lifetime and turn an otherwise healthy detached pipeline into
    // "Network connection lost", followed by retries on the same shard.
    // Keep Worker-side debug logs, but always use the detached container path
    // for focused-full production work.
    debug: isFocusedSkipDedup ? false : message.body.debug,
    force: shouldForcePipelineStart(message),
    keibajoCode,
    mode,
    raceBango,
    runYmd,
  });
  const predictUrl =
    marketSignalHook?.status === "ready"
      ? addMarketSignalAttestationToUrl(basePredictUrl, marketSignalHook.attestation)
      : basePredictUrl;
  const doId = containerRoute.namespace.idFromName(predictDoName);
  const stub = containerRoute.namespace.get(doId);
  const watchPayload: FocusedFullWatchPayload | undefined =
    isFocusedSkipDedup &&
    env.FOCUSED_FULL_WATCH_ENABLED === ENABLED_FLAG &&
    env.FOCUSED_FULL_COMPLETION_QUEUE !== undefined
      ? {
          body: message.body,
          doName: predictDoName,
          role: containerRoute.role,
          watchId: `${workKey}:${message.id}`,
          workKey,
        }
      : undefined;
  const predictHeaders = new Headers();
  if (watchPayload !== undefined) {
    predictHeaders.set(WATCH_REQUEST_HEADER, JSON.stringify(watchPayload));
  }
  const slotHold = { keep: false };
  const lifecycle: ContainerRequestLifecycle = { started: false, terminal: false };
  try {
    debugLog(
      message.body,
      `[predict-queue] container-fetch start ${describePredictMessage(
        message.body,
      )} doName=${predictDoName} url=${predictUrl}`,
    );
    lifecycle.started = true;
    await recordPreweightGenerationStartedBestEffort(env, message.body);
    const response = await stub.fetch(new Request(predictUrl, { headers: predictHeaders }));
    const focusedFullWatchId = response.headers.get(WATCH_RESPONSE_HEADER) ?? undefined;
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
    if (await retryDayBaseRequiredFailClosed(message, env, containerRoute.role, result)) {
      lifecycle.terminal = true;
      return;
    }
    if (
      isFocusedSkipDedup &&
      (await handleFocusedFullStatus({
        doName: predictDoName,
        env,
        message,
        role: containerRoute.role,
        slotHold,
        status: result.status,
        watchId: focusedFullWatchId,
        workKey,
      }))
    ) {
      return;
    }
    lifecycle.terminal = true;
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
      const cachePresent = await pickUpAndConfirmFocusedFullCache({
        env,
        message: message.body,
        role: containerRoute.role,
      });
      if (
        !cachePresent &&
        !(await pickUpAndConfirmFocusedFullCache({
          env,
          message: message.body,
          role: containerRoute.role,
        }))
      ) {
        slotHold.keep = true;
        await recoverFocusedFullCacheAfterSuccess({
          body: message.body,
          doName: predictDoName,
          env,
          message,
          role: containerRoute.role,
          workKey,
        });
        return;
      }
      await completeFocusedFullRace({
        category,
        env,
        keibajoCode: message.body.keibajoCode,
        raceBango: message.body.raceBango,
        runYmd,
        status: "success",
      });
    }
    await recordCompletedBestEffort(env, message.body);
    if (isFocusedSkipDedup) {
      if (!(await ensureViewerPredictionCacheRepair(env, message, message.body))) {
        slotHold.keep = true;
        await tryHandOffTerminalContainerStop({
          doName: predictDoName,
          env,
          role: containerRoute.role,
          workKey,
        });
        return;
      }
      await recordPreweightDisplayCompletedBestEffort(env, message.body);
    }
    slotHold.keep = true;
    const cleanupParams: TerminalContainerStopParams = {
      doName: predictDoName,
      env,
      role: containerRoute.role,
      workKey,
    };
    const cleanupHandedOff = isFocusedSkipDedup
      ? await tryHandOffTerminalContainerStop(cleanupParams)
      : await handOffTerminalContainerStop(cleanupParams);
    if (!cleanupHandedOff) {
      message.retry({ delaySeconds: CONTAINER_SLOT_RETRY_DELAY_SECONDS });
      return;
    }
    message.ack();
    console.log(
      `[predict-queue] ack ${describePredictMessage(message.body)} status=${
        result.status ?? RESULT_SUCCESS_STATUS
      } racesPredicted=${result.racesPredicted} durationMs=${Date.now() - startedAt}`,
    );
    // Display publication is deliberately restricted to the durable per-race
    // repair path above. Never start category-wide KV publication or warm work
    // after ack: that can race a newer Neon generation and the Worker may end
    // before either write completes.
  } catch (err) {
    lifecycle.terminal = lifecycle.started;
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
    if (lifecycle.started && lifecycle.terminal && !slotHold.keep) {
      slotHold.keep = true;
      await handOffTerminalContainerStop({
        doName: predictDoName,
        env,
        role: containerRoute.role,
        workKey,
      });
    }
    if (!slotHold.keep) {
      await releaseContainerSlotBestEffort({
        doName: predictDoName,
        env,
        kind: FOCUSED_FULL_SLOT_KIND,
        workKey,
      });
    }
  }
};

export const isFocusedFullCompletionQueueMessage = (
  message: Message<PredictQueueBody>,
): message is Message<FocusedFullCompletionMessage> =>
  "type" in message.body && message.body.type === "focused-full-completion";

export const isFocusedFullWatchTickQueueMessage = (
  message: Message<PredictQueueBody>,
): message is Message<FocusedFullWatchTickMessage> =>
  "type" in message.body && message.body.type === "focused-full-watch-tick";

export const consumeFocusedFullWatchTick = async (
  message: Message<FocusedFullWatchTickMessage>,
  env: Env,
): Promise<void> => {
  if (!isFocusedSkipDedupMessage(message.body.body)) {
    message.ack();
    return;
  }
  let completion: FocusedFullCompletionMessage | undefined;
  try {
    const namespace = resolveContainerNamespaceForRole(env, message.body.role);
    const stub = namespace.get(namespace.idFromName(message.body.doName));
    completion = await pollFocusedFullWatchTick(
      { ...message.body, body: message.body.body },
      {
        now: Date.now,
        pollStatus: (body) => stub.fetch(new Request(buildWatchStatusUrl(body))),
      },
    );
  } catch (error) {
    console.error(
      `[predict-queue] focused-full watch tick failed watchId=${message.body.watchId}:`,
      String(error),
    );
    throw error;
  }
  if (completion === undefined) {
    await touchContainerSlot({
      doName: message.body.doName,
      env,
      staleAfterMs: CONTAINER_SLOT_STALE_MS,
      workKey: message.body.workKey,
    });
    await sendFocusedFullWatchMessageDurably(env, message.body, FOCUSED_FULL_WATCH_POLL_SECONDS);
  } else {
    await sendFocusedFullWatchMessageDurably(env, completion);
  }
  message.ack();
};

const makeDeferredPredictMessage = (
  message: Message<FocusedFullCompletionMessage>,
  decision: DeferredMessageDecision,
): Message<PredictQueueMessage> => ({
  ack() {
    decision.acknowledged = true;
  },
  attempts: message.attempts,
  body: message.body.body,
  id: message.id,
  retry(options) {
    decision.retried = true;
    decision.retryOptions = options;
  },
  timestamp: message.timestamp,
});

const enqueueFocusedFullTerminalErrorRecovery = async (
  message: Message<FocusedFullCompletionMessage>,
  env: Env,
): Promise<void> => {
  await env.PREDICT_QUEUE.send(
    {
      ...message.body.body,
      force: true,
      forceRequestedAt: new Date().toISOString(),
    },
    { delaySeconds: CONTAINER_SLOT_RETRY_DELAY_SECONDS },
  );
};

export const consumeFocusedFullCompletion = async (
  message: Message<FocusedFullCompletionMessage>,
  env: Env,
): Promise<void> => {
  const claimId = message.id;
  const claim = await claimFocusedFullTerminalWatch({
    claimId,
    env,
    staleAfterMs: FOCUSED_FULL_TERMINAL_CLAIM_STALE_MS,
    watchId: message.body.watchId,
  });
  if (!claim.proceed) {
    if (claim.state !== "terminal") {
      await sendFocusedFullWatchMessageDurably(
        env,
        message.body,
        FOCUSED_FULL_WATCH_BACKUP_SECONDS,
      );
    }
    message.ack();
    return;
  }
  if (!isFocusedSkipDedupMessage(message.body.body)) {
    await completeFocusedFullTerminalWatch({ claimId, env, watchId: message.body.watchId });
    message.ack();
    return;
  }
  try {
    await sendFocusedFullWatchMessageDurably(env, message.body, FOCUSED_FULL_WATCH_BACKUP_SECONDS);
  } catch (error) {
    console.error(
      `[predict-queue] focused-full terminal watchdog enqueue failed watchId=${message.body.watchId}:`,
      String(error),
    );
    throw error;
  }
  const decision: DeferredMessageDecision = { acknowledged: false, retried: false };
  const predictMessage = makeDeferredPredictMessage(message, decision);
  const common: CompleteFocusedFullFromStatusParams = {
    body: message.body.body,
    doName: message.body.doName,
    env,
    message: predictMessage,
    role: message.body.role,
    synchronousStop: true,
    workKey: message.body.workKey,
  };
  let stopped = claim.state === "stopped";
  const terminalStopMessage: ContainerControlMessage = {
    name: message.body.doName,
    requestedAt: new Date().toISOString(),
    role: message.body.role,
    type: "container-stop",
    workKey: message.body.workKey,
  };
  const markWatchStopped = (): Promise<void> =>
    markFocusedFullTerminalWatchStopped({ claimId, env, watchId: message.body.watchId });
  const stopTerminalContainer = async (): Promise<void> => {
    const stopCompleted = await consumeContainerStop(env, terminalStopMessage, markWatchStopped);
    if (!stopCompleted) {
      console.warn(
        `[predict-queue] focused-full terminal stop was not owned; treating completion as terminal doName=${message.body.doName} workKey=${message.body.workKey}`,
      );
    }
    stopped = true;
  };
  if (stopped) {
    await consumeContainerStop(env, terminalStopMessage, markWatchStopped);
  }
  if (message.body.outcome === "success") {
    try {
      await completeFocusedFullFromStatus(common);
    } catch (error) {
      if (
        !(error instanceof Error) ||
        error.message !== "FOCUSED_FULL_WATCH_CACHE_MISSING_AFTER_SUCCESS"
      )
        throw error;
      if (!stopped) await stopTerminalContainer();
      await recoverFocusedFullCacheAfterSuccess(common);
    }
  } else if (message.body.outcome === "missing" || message.body.outcome === "timeout") {
    if (!stopped) await stopTerminalContainer();
    await recoverFocusedFullCache({
      body: message.body.body,
      env,
      message: predictMessage,
      reason: "missing-status",
    });
  } else {
    if (!stopped) await stopTerminalContainer();
    await recoverFocusedFullStatus({
      ...common,
      error: message.body.error ?? "Focused-full detached pipeline failed",
    });
    if (decision.retried && !decision.acknowledged) {
      try {
        await enqueueFocusedFullTerminalErrorRecovery(message, env);
        decision.acknowledged = true;
        decision.retried = false;
      } catch (error) {
        console.error(
          `[predict-queue] focused-full terminal recovery enqueue failed watchId=${message.body.watchId}:`,
          String(error),
        );
      }
    }
  }
  if (decision.retried || !decision.acknowledged) {
    throw new Error(
      `Focused-full terminal finalizer incomplete watchId=${message.body.watchId} retryDelaySeconds=${decision.retryOptions?.delaySeconds ?? FOCUSED_FULL_RETRY_DELAY_SECONDS}`,
    );
  }
  if (!stopped) await stopTerminalContainer();
  await completeFocusedFullTerminalWatch({ claimId, env, watchId: message.body.watchId });
  message.ack();
};

export const handleQueue = async (
  batch: MessageBatch<PredictQueueBody>,
  env: Env,
): Promise<void> => {
  for (const message of batch.messages) {
    if (isFocusedFullWatchTickQueueMessage(message)) {
      await consumeFocusedFullWatchTick(message, env);
    } else if (isFocusedFullCompletionQueueMessage(message)) {
      await consumeFocusedFullCompletion(message, env);
    } else if (isDeliveryCanaryQueueMessage(message)) {
      await consumeDeliveryCanary(env, message.body, new Date());
      message.ack();
    } else if (isPredictionCacheRepairQueueMessage(message)) {
      await consumePredictionCacheRepair(message, env);
    } else if (isContainerCleanupQueueMessage(message)) {
      await consumeContainerCleanup({ env, message: message.body });
      message.ack();
    } else if (isDayBasePickupQueueMessage(message)) {
      await consumeDayBasePickup({ env, message: message.body });
      if (isOldDateRunYmd(message.body.runYmd, new Date()))
        await clearDayBaseRepairReservation({
          category: message.body.category,
          env,
          runYmd: message.body.runYmd,
        });
      message.ack();
    } else if (isDayBasePrewarmQueueMessage(message)) {
      if (isOldDateRunYmd(message.body.runYmd, new Date())) {
        await handlePastDayBaseSkip(message, env);
        continue;
      }
      const outcome = await prewarmCategoryWithOutcome({
        category: message.body.category,
        daysAhead: message.body.daysAhead,
        env,
        ...dayBaseGenerationFields(message.body.generationId),
        ...(message.body.generatePredictionsAfterHit === true
          ? { generatePredictionsAfterHit: true }
          : {}),
        ...(message.body.force === true ? { force: true } : {}),
        runYmd: message.body.runYmd,
      });
      if (outcome === "landed" || outcome === "pickup-scheduled" || outcome === "superseded")
        message.ack();
      else message.retry({ delaySeconds: 30 });
    } else if (isPredictQueueMessage(message)) {
      await processMessage(message, env);
    }
  }
};
