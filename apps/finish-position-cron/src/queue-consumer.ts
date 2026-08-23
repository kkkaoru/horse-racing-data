// Run with bun. Queue consumer: processes one predict message per batch invocation.
// For each message: dedup via DO coordinator (strong consistency), call the Container
// DO stub's fetch, track state.

import {
  cleanupDayBaseWork,
  consumeDayBasePickup,
  isDayBasePickupQueueMessage,
} from "./day-base-pickup";
import {
  consumeDeliveryCanary,
  isDeliveryCanaryQueueMessage,
  isPredictQueueMessage,
} from "./delivery-canary";
import { recordDeliveryConsumed, recordPredictionCompleted } from "./delivery-lifecycle";
import {
  CONTAINER_SLOT_CAPPED_STATE,
  CONTAINER_SLOT_RETRY_DELAY_SECONDS,
  CONTAINER_SLOT_STALE_MS,
  type ContainerSlotKind,
} from "./container-slot-cap";
import {
  consumeContainerCleanup,
  handOffContainerStopOrCleanup,
  isContainerCleanupQueueMessage,
} from "./container-cleanup";
import { isDayBasePrewarmQueueMessage, prewarmCategoryWithOutcome } from "./day-base-prewarm";
import {
  claimContainerSlot,
  claimFocusedFullRace,
  claimRescoreExecution,
  claimRun,
  clearContainerSlot,
  completeFocusedFullRace,
  completeRescoreRace,
  completeRun,
  failFocusedFullRaceEnqueue,
  releaseContainerSlot,
  reserveFocusedFullRaceEnqueue,
  touchContainerSlot,
} from "./do-state";
import {
  isFocusedFullPredictionComplete,
  isPerRaceFeatureCachePresent,
  isPerRaceRescoreReady,
} from "./focused-full-completion";
import { pickUpFocusedFullCache } from "./focused-full-cache-pickup";
import { clearDayBaseRepairReservation, enqueueDayBaseRepairOnce } from "./day-base-repair";
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
import {
  qualifyPredictionContainerDoName,
  resolveRaceContainerRoute,
  type PredictionContainerRole,
} from "./race-container-routing";
import { resolveCardMaxRaceBangoForKochi } from "./race-coordinator";
import { addRescoreAttestationToUrl, createRescoreAttestation } from "./rescore-attestation";
import { getRunningStyleRaceReadiness } from "./running-style-readiness";
import { rescoreJraRace } from "./scoring/rescore-consumer";
import {
  buildRetryErrorBindParams,
  buildRetryErrorInsertSql,
  buildRetryErrorRecord,
} from "./retry-errors";
import type {
  DayBasePickupMessage,
  DayBasePrewarmMessage,
  Env,
  PredictCategory,
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
// Focused full returns "accepted" when the container either launched a detached
// pipeline for this race or observed the same race already in flight. The queue
// consumer polls Neon completion on delayed redeliveries instead of holding a
// Cloudflare request open for the whole feature chain.
const FOCUSED_FULL_ACCEPTED_STATUS = "accepted";
const FOCUSED_FULL_BUSY_STATUS = "busy";
const FOCUSED_FULL_ALREADY_COMPLETE_STATUS = "already-complete";
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
const RESCORE_EXECUTION_STALE_MS = 31 * 60 * 1000;
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
  runYmd: string;
}

type FocusedFullPollStatus = "error" | "missing" | "running" | "success";

interface FocusedFullStatusPayload {
  error: string | null;
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
  status: "error" | "missing";
  workKey: string;
}

interface CompleteFocusedFullFromStatusParams {
  body: FocusedFullSkipDedupMessage;
  doName: string;
  env: Env;
  message: Message<PredictQueueMessage>;
  role: PredictionContainerRole;
  workKey: string;
}

interface FocusedFullStatusTarget {
  doName: string;
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

interface HandleFocusedFullStatusParams {
  doName: string;
  env: Env;
  message: Message<PredictQueueMessage>;
  role: PredictionContainerRole;
  slotHold: { keep: boolean };
  status: string | undefined;
  workKey: string;
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
): Promise<void> => {
  if (published.status !== "written") return;
  const warmParams: ReturnType<typeof buildWarmRaceParamsFromYmd> = buildWarmRaceParamsFromYmd(
    params.runYmd,
    params.keibajoCode,
    params.raceBango,
  );
  await warmViewerDisplayForRace({
    ...warmParams,
    refresh: true,
    viewer: env.PC_KEIBA_VIEWER,
  });
};

const populateViewerDisplayForFocusedRace = async (
  env: Env,
  message: FocusedFullSkipDedupMessage,
): Promise<void> => {
  const published = await publishPredictionKvForFocusedRace(env, message, true);
  await warmViewerDisplayAfterKvWrite(env, published, message);
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

// `force` means "run once even if Neon already has rows", not "rerun on every
// Queue delivery". Focused-full work intentionally returns `accepted` and is
// redelivered while its detached Container thread finishes. Keeping force
// active on those redeliveries bypasses both completion guards and starts the
// same expensive pipeline again before the in-memory feat-cache payload can be
// picked up. Cloudflare numbers the first delivery as attempt 1; tolerate an
// absent attempts value for local/backward-compatible Message implementations.
const isInitialForcedDelivery = (message: Message<PredictQueueMessage>): boolean =>
  message.body.force === true && (message.attempts === undefined || message.attempts <= 1);

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
  params: EnqueueFocusedFullCacheRepairParams,
): Promise<void> => {
  const { body, check, env, message } = params;
  const error =
    check.status.status === "missing"
      ? `Focused-full status missing after Container recreation: ${check.status.raceKey}`
      : (check.status.error ?? `Focused-full detached pipeline failed: ${check.status.raceKey}`);
  await persistRetryError(env, message, new Error(error));
  await completeFocusedFullRace({
    category: body.category,
    env,
    keibajoCode: body.keibajoCode,
    raceBango: body.raceBango,
    runYmd: body.runYmd,
    status: "error",
  });
  await handOffTerminalContainerStop({
    doName: check.doName,
    env,
    role: check.role,
    workKey: check.workKey,
  });

  const reservationId = crypto.randomUUID();
  const reservation = await reserveFocusedFullRaceEnqueue({
    category: body.category,
    env,
    keibajoCode: body.keibajoCode,
    raceBango: body.raceBango,
    raceStartAtJst: body.raceStartAtJst,
    reservationId,
    runYmd: body.runYmd,
    staleAfterMs: FOCUSED_FULL_IN_FLIGHT_STALE_MS,
  });
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
        await failFocusedFullRaceEnqueue({
          category: body.category,
          env,
          keibajoCode: body.keibajoCode,
          raceBango: body.raceBango,
          reservationId,
          runYmd: body.runYmd,
        });
      } catch (releaseError) {
        console.error(
          "[predict-queue] failed to release focused-full cache-repair reservation:",
          String(releaseError),
        );
      }
      throw error;
    }
  }
  message.ack();
  console.warn(
    `[predict-queue] focused-full cache repair ${
      reservation.proceed ? "enqueued" : "already-reserved"
    } ${describePredictMessage(body)}`,
  );
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
      message.retry({ delaySeconds: CONTAINER_SLOT_RETRY_DELAY_SECONDS });
      return;
    }
    await enqueueFocusedFullCacheRepair({ body, check, env, message });
  } catch (error) {
    await handOffTerminalContainerStop({ ...target, env });
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
  if (isInitialForcedDelivery(message)) {
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
    await populateViewerDisplayForFocusedRace(env, message.body);
    if (!(await handOffTerminalContainerStop({ doName, env, role: route.role, workKey }))) {
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
  return { error, raceKey, status };
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
  await handOffTerminalContainerStop({
    doName: params.doName,
    env: params.env,
    role: params.role,
    workKey: params.workKey,
  });
  if (
    params.role === "race-chain" &&
    params.status === "error" &&
    params.error.includes(DAY_BASE_REQUIRED_ERROR_CODE)
  ) {
    try {
      await params.env.PREDICT_QUEUE.send(
        { ...params.body, forceLegacyContainer: true },
        { delaySeconds: CONTAINER_SLOT_RETRY_DELAY_SECONDS },
      );
    } catch (error) {
      params.message.retry({ delaySeconds: FOCUSED_FULL_RETRY_DELAY_SECONDS });
      console.warn(
        `[predict-queue] detached race-chain legacy fallback enqueue failed ${describePredictMessage(
          params.message.body,
        )}:`,
        String(error),
      );
      return;
    }
    params.message.ack();
    console.warn(
      `[predict-queue] detached race-chain requested legacy fallback ${describePredictMessage(
        params.message.body,
      )} delaySeconds=${CONTAINER_SLOT_RETRY_DELAY_SECONDS}`,
    );
    return;
  }
  params.message.retry({ delaySeconds: CONTAINER_SLOT_RETRY_DELAY_SECONDS });
  console.warn(
    `[predict-queue] focused-full status ${params.status}; cleanup handed off before retry ${describePredictMessage(
      params.message.body,
    )}; error=${params.error}`,
  );
};

const completeFocusedFullFromStatus = async (
  params: CompleteFocusedFullFromStatusParams,
): Promise<void> => {
  const { body, doName, env, message, role, workKey } = params;
  if (!(await pickUpAndConfirmFocusedFullCache({ env, message: body, role }))) {
    message.retry({ delaySeconds: FOCUSED_FULL_RETRY_DELAY_SECONDS });
    return;
  }
  await completeFocusedFullRace({
    category: body.category,
    env,
    keibajoCode: body.keibajoCode,
    raceBango: body.raceBango,
    runYmd: body.runYmd,
    status: "success",
  });
  await recordCompletedBestEffort(env, body);
  await populateViewerDisplayForFocusedRace(env, body);
  if (!(await handOffTerminalContainerStop({ doName, env, role, workKey }))) {
    message.retry({ delaySeconds: CONTAINER_SLOT_RETRY_DELAY_SECONDS });
    return;
  }
  message.ack();
};

const pollFocusedFullStatus = async (
  message: Message<PredictQueueMessage>,
  body: FocusedFullSkipDedupMessage,
  env: Env,
): Promise<void> => {
  const target = await resolveFocusedFullStatusTarget(body, env);
  try {
    const { doName, role, workKey } = target;
    const status = await target.fetchStatus();
    if (status.status === "running") {
      await touchContainerSlot({
        doName,
        env,
        staleAfterMs: FOCUSED_FULL_IN_FLIGHT_STALE_MS,
        workKey,
      });
      retryFocusedFullAlreadyInFlight(message);
      return;
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
      return;
    }
    const error =
      status.status === "missing"
        ? `Focused-full status missing after Container recreation: ${status.raceKey}`
        : (status.error ?? `Focused-full detached pipeline failed: ${status.raceKey}`);
    await recoverFocusedFullStatus({
      body,
      doName,
      env,
      error,
      message,
      role,
      status: status.status,
      workKey,
    });
  } catch (error) {
    await handOffTerminalContainerStop({ ...target, env });
    console.warn(
      `[predict-queue] focused-full status query failed ${describePredictMessage(message.body)}:`,
      String(error),
    );
    retryFocusedFullAlreadyInFlight(message);
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
    force: isInitialForcedDelivery(message),
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
      await pollFocusedFullStatus(message, body, env);
      return false;
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
    await pollFocusedFullStatus(message, body, env);
    return false;
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

const retryAfterFailure = async (
  message: Message<PredictQueueMessage>,
  env: Env,
  err: unknown,
): Promise<void> => {
  await persistRetryError(env, message, err);
  message.retry({ delaySeconds: computeRetryDelaySeconds(message.attempts) });
};

const deferFocusedFullUntilRunningStyleReady = async (
  message: Message<PredictQueueMessage>,
  env: Env,
): Promise<boolean> => {
  if (!isFocusedSkipDedupMessage(message.body)) return false;
  const { category, keibajoCode, raceBango, runYmd } = message.body;
  try {
    const [readiness] = await getRunningStyleRaceReadiness({
      category,
      db: env.REALTIME_DB,
      races: [{ category, keibajoCode, raceBango }],
      runYmd,
    });
    if (readiness?.reason === null) return false;
    const reason = readiness?.reason ?? "state-missing";
    const delaySeconds = computeRetryDelaySeconds(message.attempts);
    message.retry({ delaySeconds });
    console.warn(
      `[predict-queue] focused-full deferred before claim ${describePredictMessage(
        message.body,
      )} reason=running-style-${reason} attempts=${message.attempts} delaySeconds=${delaySeconds}`,
    );
    return true;
  } catch (error) {
    console.error(
      `[predict-queue] focused-full running-style readiness failed before claim ${describePredictMessage(
        message.body,
      )}:`,
      String(error),
    );
    await retryAfterFailure(message, env, error);
    return true;
  }
};

const deferFocusedFullUntilDayBaseReady = async (
  message: Message<PredictQueueMessage>,
  env: Env,
): Promise<boolean> => {
  if (!isFocusedSkipDedupMessage(message.body)) return false;
  const { category, runYmd } = message.body;
  try {
    const readiness = await getFocusedFullDayBaseReadiness({ category, env, runYmd });
    if (readiness.ready) {
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
    const repair = await enqueueDayBaseRepairOnce({ category, env, runYmd });
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
  const delaySeconds = computeRetryDelaySeconds(message.attempts);
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
  const { doName, env, message, role, slotHold, status, workKey } = params;
  const { category, keibajoCode, raceBango, runYmd } = message.body;
  const suffix = raceScopeSuffix(keibajoCode, raceBango);
  if (status === FOCUSED_FULL_ACCEPTED_STATUS) {
    debugLog(
      message.body,
      `Focused full accepted, still in progress category=${category} runYmd=${runYmd}${suffix} -- will re-check on redelivery`,
    );
    message.retry({ delaySeconds: FOCUSED_FULL_RETRY_DELAY_SECONDS });
    slotHold.keep = FOCUSED_FULL_ACCEPTED_KEEP_SLOT;
    return true;
  }
  if (status === FOCUSED_FULL_ALREADY_COMPLETE_STATUS) {
    debugLog(
      message.body,
      `Focused full already complete (container) category=${category} runYmd=${runYmd}${suffix}`,
    );
    if (isFocusedSkipDedupMessage(message.body)) {
      if (!(await pickUpAndConfirmFocusedFullCache({ env, message: message.body, role }))) {
        message.retry({ delaySeconds: FOCUSED_FULL_RETRY_DELAY_SECONDS });
        slotHold.keep = true;
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
      await populateViewerDisplayForFocusedRace(env, message.body);
      slotHold.keep = await handOffTerminalContainerStop({ doName, env, role, workKey });
      if (!slotHold.keep) {
        message.retry({ delaySeconds: CONTAINER_SLOT_RETRY_DELAY_SECONDS });
        slotHold.keep = true;
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

const handOffDayBaseRequiredToLegacy = async (
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
  await env.PREDICT_QUEUE.send(
    { ...message.body, forceLegacyContainer: true },
    { delaySeconds: CONTAINER_SLOT_RETRY_DELAY_SECONDS },
  );
  message.ack();
  console.warn(
    `[predict-queue] race-chain requested legacy fallback ${describePredictMessage(
      message.body,
    )} delaySeconds=${CONTAINER_SLOT_RETRY_DELAY_SECONDS}`,
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
  const { category, daysAhead, debug, keibajoCode, raceBango, runYmd } = message.body;
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
    runYmd,
  });
  const doId = env.FINISH_POSITION_PREDICT_CONTAINER.idFromName(predictDoName);
  const stub = env.FINISH_POSITION_PREDICT_CONTAINER.get(doId);
  const lifecycle: ContainerRequestLifecycle = { started: false, terminal: false };
  const cleanupHandedOff = { value: false };
  try {
    const [commitResult] = await Promise.allSettled([
      (async () => {
        const attestation = await createRescoreAttestation({
          category,
          env,
          keibajoCode,
          raceBango,
          runYmd,
        });
        const predictUrl = addRescoreAttestationToUrl(basePredictUrl, attestation);
        debugLog(
          message.body,
          `[predict-queue] container-fetch start ${describePredictMessage(
            message.body,
          )} doName=${predictDoName} url=${predictUrl}`,
        );
        lifecycle.started = true;
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
        const published = await publishPredictionKvForRace(env, {
          bustCacheApi: true,
          category,
          keibajoCode,
          raceBango,
          runYmd,
        });
        await completeRescoreExecution(message, env, "success");
        return published;
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
    await handOffTerminalContainerStop({
      doName: predictDoName,
      env,
      role: "legacy",
      workKey: buildPredictWorkKey(message.body),
    });
    cleanupHandedOff.value = true;
    try {
      await warmViewerDisplayAfterKvWrite(env, commitResult.value, {
        keibajoCode,
        raceBango,
        runYmd,
      });
    } catch (error) {
      console.error(
        `[predict-queue] committed rescore viewer warm failed ${describePredictMessage(
          message.body,
        )}:`,
        String(error),
      );
    }
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
    const result = await rescoreJraRace({ env, fetchImpl: fetch, message: message.body });
    if (result.status !== "ok") {
      throw new Error(`Worker rescore unavailable: ${result.status}`);
    }
    const published = await publishPredictionKvForRace(env, {
      bustCacheApi: true,
      category,
      keibajoCode,
      raceBango,
      runYmd,
    });
    await completeRescoreExecution(message, env, "success");
    try {
      await warmViewerDisplayAfterKvWrite(env, published, { keibajoCode, raceBango, runYmd });
    } catch (error) {
      console.error(
        `[predict-queue] committed Worker rescore viewer warm failed ${describePredictMessage(
          message.body,
        )}:`,
        String(error),
      );
    }
    console.log(
      `Rescore Worker category=${category} runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango} races=${result.racesPredicted} predictions=${result.predictionCount} model=${result.modelVersion} durationMs=${Date.now() - startedAt}`,
    );
    message.ack();
  } catch (error) {
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
  message: Message<DayBasePickupMessage | DayBasePrewarmMessage>,
  env: Env,
): Promise<void> => {
  await cleanupPastDayBaseWork({
    category: message.body.category,
    env,
    runYmd: message.body.runYmd,
  });
  message.ack();
  console.warn(
    `[predict-queue] skipping old day-base message type=${message.body.type} category=${message.body.category} runYmd=${message.body.runYmd}`,
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
  const workKey = isFocusedSkipDedup
    ? buildFocusedFullWorkKey(message.body)
    : buildPredictWorkKey(message.body);
  const shouldCompleteCategoryRun = !isFocusedSkipDedup;
  const shouldWarmCategoryCache =
    skipDedup === true && shouldCompleteCategoryRun && mode !== RESCORE_MODE;
  const cardMaxRaceBango = await resolveCardMaxRaceBangoForKochi({ env, keibajoCode, runYmd });
  if (await deferFocusedFullUntilDayBaseReady(message, env)) return;
  if (await deferFocusedFullUntilRunningStyleReady(message, env)) return;
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
  const containerRoute = await resolveRaceContainerRoute({
    category,
    env,
    forceLegacy: message.body.forceLegacyContainer,
    focusedFull: isFocusedSkipDedup,
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
  const predictUrl = buildPredictUrl({
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
    force: isInitialForcedDelivery(message),
    keibajoCode,
    mode,
    raceBango,
    runYmd,
  });
  const doId = containerRoute.namespace.idFromName(predictDoName);
  const stub = containerRoute.namespace.get(doId);
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
    if (await handOffDayBaseRequiredToLegacy(message, env, containerRoute.role, result)) {
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
      if (
        !(await pickUpAndConfirmFocusedFullCache({
          env,
          message: message.body,
          role: containerRoute.role,
        }))
      ) {
        message.retry({ delaySeconds: FOCUSED_FULL_RETRY_DELAY_SECONDS });
        slotHold.keep = true;
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
      await populateViewerDisplayForFocusedRace(env, message.body);
    }
    slotHold.keep = true;
    await handOffTerminalContainerStop({
      doName: predictDoName,
      env,
      role: containerRoute.role,
      workKey,
    });
    message.ack();
    console.log(
      `[predict-queue] ack ${describePredictMessage(message.body)} status=${
        result.status ?? RESULT_SUCCESS_STATUS
      } racesPredicted=${result.racesPredicted} durationMs=${Date.now() - startedAt}`,
    );
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

export const handleQueue = async (
  batch: MessageBatch<PredictQueueBody>,
  env: Env,
): Promise<void> => {
  for (const message of batch.messages) {
    if (isDeliveryCanaryQueueMessage(message)) {
      await consumeDeliveryCanary(env, message.body, new Date());
      message.ack();
    } else if (isContainerCleanupQueueMessage(message)) {
      await consumeContainerCleanup({ env, message: message.body });
      message.ack();
    } else if (isDayBasePickupQueueMessage(message)) {
      if (message.body.force !== true && isOldDateRunYmd(message.body.runYmd, new Date())) {
        await handlePastDayBaseSkip(message, env);
        continue;
      }
      await consumeDayBasePickup({ env, message: message.body });
      message.ack();
    } else if (isDayBasePrewarmQueueMessage(message)) {
      if (message.body.force !== true && isOldDateRunYmd(message.body.runYmd, new Date())) {
        await handlePastDayBaseSkip(message, env);
        continue;
      }
      const outcome = await prewarmCategoryWithOutcome({
        category: message.body.category,
        daysAhead: message.body.daysAhead,
        env,
        ...(message.body.generatePredictionsAfterHit === true
          ? { generatePredictionsAfterHit: true }
          : {}),
        ...(message.body.force === true ? { force: true } : {}),
        runYmd: message.body.runYmd,
      });
      if (outcome === "landed" || outcome === "pickup-scheduled") message.ack();
      else message.retry({ delaySeconds: 30 });
    } else if (isPredictQueueMessage(message)) {
      await processMessage(message, env);
    }
  }
};
