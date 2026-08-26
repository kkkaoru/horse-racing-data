// Run with bun. Fetch (health + on-demand trigger) + scheduled (cron -> container) + queue handlers.

import {
  FinishPositionPredictContainer,
  FinishPositionRaceChainContainer,
} from "./container-class";
import {
  refreshCornerFeatures,
  shouldRunCornerFeaturesRefreshCron,
} from "./corner-features-refresh";
import { runCoverageSelfHeal, shouldRunCoverageSelfHealCron } from "./coverage-self-heal";
import {
  PREDICT_CRON,
  shouldRunCoordinatorCron,
  shouldRunFeatureBuildCron,
  shouldRunPredictCron,
  shouldRunRescoreCron,
  shouldRunWarmCron,
} from "./cron-decision";
import { prewarmCategoryWithOutcome, runDayBasePrewarm } from "./day-base-prewarm";
import { pickUpPrewarmDayBase } from "./day-base-prewarm-pickup";
import { completeLandedDayBase } from "./day-base-pickup";
import { materializeDayBasePerRaceCache } from "./day-base-race-materializer";
import {
  enqueueDeliveryCanary,
  listDeliveryCanaries,
  shouldRunDeliveryCanaryCron,
} from "./delivery-canary";
import { DLQ_QUEUE_NAME, handleDlqQueue } from "./dlq-consumer";
import {
  consumeContainerStop,
  isAllowedContainerDoName,
  isContainerControlQueueMessage,
} from "./container-control";
import { claimRescoreRace, completeFocusedFullRace, releaseRescoreRaceClaim } from "./do-state";
import { recordPreweightGenerationStarted } from "./delivery-lifecycle";
import { warmNeon } from "./neon-warm";
import { resolvePredictDoName } from "./predict-do-shard";
import {
  handlePredictDoStatePurge,
  PREDICT_DO_INTERNAL_PURGE_PATH,
} from "./predict-do-state-purge";
import { PredictRunCoordinator } from "./predict-run-coordinator";
import {
  hasRequiredPerRaceScope,
  normalizePerRaceScope,
  PER_RACE_SCOPE_REQUIRED_ERROR,
} from "./per-race-scope-guard";
import { getPredictionReadiness } from "./prediction-readiness";
import { getFocusedFullDayBaseReadiness } from "./focused-full-day-base-readiness";
import type { PredictionContainerRole } from "./race-container-routing";
import { handleQueue } from "./queue-consumer";
import { enqueuePredict } from "./queue-producer";
import { WATCH_REQUEST_HEADER } from "./focused-full-watch";
import { DEFAULT_RESCORE_LEAD_MINUTES, runRaceCoordinatorTick } from "./race-coordinator";
import {
  runRunningStyleKickMorningGap,
  runRunningStyleKickTomorrowPrewarm,
  shouldRunRunningStyleKickMorningGapCron,
  shouldRunRunningStyleKickTomorrowPrewarmCron,
} from "./running-style-kick";
import { getRunDateJst, getRunYmdJst } from "./time";
import { isAuthorized, isTriggerRequest, parseRunDates } from "./trigger";
import type {
  ContainerControlMessage,
  Env,
  PredictQueueBody,
  PredictCategory,
  PredictMode,
  PredictQueueMessage,
  FocusedFullWatchPayload,
  RunDates,
} from "./types";

// Conservative default (no backward window) when env.CORNER_FEATURES_LOOKBACK_DAYS
// is unset -- matches refreshCornerFeatures's own forward-only default so an
// environment/test fixture missing this var behaves exactly as it did before
// the lookback feature was added.
const CORNER_FEATURES_NO_LOOKBACK_DAYS = 0;
const RUN_DATE_FIELD = "runDate";
const MODE_FIELD = "mode";
const CATEGORY_FIELD = "category";
const KEIBAJO_CODE_FIELD = "keibajoCode";
const RACE_BANGO_FIELD = "raceBango";
const SKIP_DEDUP_FIELD = "skipDedup";
const DEBUG_FIELD = "debug";
const FORCE_FIELD = "force";
const RUN_YMD_FIELD = "runYmd";
const RACE_START_AT_JST_FIELD = "raceStartAtJst";
const WEIGHT_SNAPSHOT_COUNT_FIELD = "weightSnapshotCount";
const WEIGHT_SNAPSHOT_FETCHED_AT_FIELD = "weightSnapshotFetchedAt";
const WEIGHT_SNAPSHOT_HASH_FIELD = "weightSnapshotHash";
const DEFAULT_MODE: PredictMode = "full";
const RESCORE_MODE: PredictMode = "rescore";
const VALID_MODES: ReadonlySet<string> = new Set(["full", "rescore"]);
const VALID_CATEGORIES: ReadonlySet<string> = new Set(["jra", "nar", "ban-ei"]);
const RESCORE_DAYS_AHEAD = 0;
const RESCORE_ENABLED_FLAG = "1";
const HTTP_OK = 200;
const HTTP_UNAUTHORIZED = 401;
const HTTP_BAD_REQUEST = 400;
const HTTP_ACCEPTED = 202;
const HTTP_SERVICE_UNAVAILABLE = 503;
const ADMIN_STOP_CONTAINERS_PATH = "/api/admin/stop-predict-containers";
const ADMIN_COMPLETE_FOCUSED_FULL_RACE_PATH = "/api/admin/complete-focused-full-race";
const ADMIN_RUN_FOCUSED_FULL_RACE_PATH = "/api/admin/run-focused-full-race";
const ADMIN_RUN_FOCUSED_FULL_RACE_DIRECT_PATH = "/api/admin/run-focused-full-race-direct";
const ADMIN_PREWARM_DAY_BASE_PATH = "/api/admin/prewarm-day-base";
const ADMIN_PREWARM_DAY_BASE_STATUS_PATH = "/api/admin/prewarm-day-base-status";
const ADMIN_PICKUP_DAY_BASE_PATH = "/api/admin/pickup-day-base";
const ADMIN_MATERIALIZE_DAY_BASE_PATH = "/api/admin/materialize-day-base-races";
const ADMIN_PURGE_UNUSED_PREDICT_DO_STATE_PATH = "/api/admin/purge-unused-predict-do-state";
export const CONTAINER_CONTROL_QUEUE_NAME = "finish-position-container-control-queue";
const MAX_ADMIN_STOP_NAMES = 100;
const INTERNAL_RESCORE_RACE_PATH = "/api/internal/rescore-race";
const INTERNAL_READINESS_PATH = "/api/internal/prediction-readiness";
const INTERNAL_CANARY_PATH = "/api/internal/delivery-canaries";
const INTERNAL_MONITOR_METHOD = "GET";
const INTERNAL_RESCORE_RACE_METHOD = "POST";
const RUN_YMD_LENGTH = 8;
const RUN_YMD_YEAR_END = 4;
const RUN_YMD_MONTH_END = 6;
const RUN_YMD_PATTERN = /^\d{8}$/u;
const SHA256_HEX_PATTERN = /^[0-9a-f]{64}$/u;
const RUN_DATE_SEPARATOR = "-";

interface RaceScopedPredictRequest {
  category: PredictCategory;
  debug?: boolean;
  // See PredictUrlParams.force in queue-consumer.ts (Defect H) -- forwarded
  // to the container so its row-count-only completion check can be bypassed
  // for an admin-triggered focused-full re-run too.
  force?: boolean;
  keibajoCode: string;
  raceBango: string;
  runYmd: string;
  raceStartAtJst?: string;
}

interface AdminContainerStopTarget {
  name: string;
  role: PredictionContainerRole;
}

interface InternalRescoreRaceRequest extends RaceScopedPredictRequest {
  activeHorseNumbers?: number[];
  excludedHorseNumbers?: number[];
  entrySnapshotFetchedAt?: string;
  entrySnapshotHash?: string;
  weightSnapshotCount: number;
  weightSnapshotFetchedAt: string;
  weightSnapshotHash: string;
}

interface RescoreRaceStartRow {
  race_start_at_jst: string;
}

interface AdminCompleteFocusedFullRaceRequest extends RaceScopedPredictRequest {
  status: "error" | "success";
}

interface AdminPrewarmDayBaseRequest {
  category?: PredictCategory;
  force?: boolean;
  generatePredictionsAfterHit?: boolean;
  runYmd: string;
}

export { FinishPositionPredictContainer, FinishPositionRaceChainContainer, PredictRunCoordinator };

const healthResponse = (): Response =>
  Response.json({ cron: PREDICT_CRON, name: "finish-position-cron", ok: true });

const resolveMode = (body: Record<string, unknown>): PredictMode => {
  const requested = body[MODE_FIELD];
  return typeof requested === "string" && VALID_MODES.has(requested)
    ? (requested as PredictMode)
    : DEFAULT_MODE;
};

const resolveCategory = (body: Record<string, unknown>): PredictCategory | undefined => {
  const requested = body[CATEGORY_FIELD];
  return typeof requested === "string" && VALID_CATEGORIES.has(requested)
    ? (requested as PredictCategory)
    : undefined;
};

// A per-race target field (keibajoCode / raceBango) is a non-empty trimmed
// string when present; anything else (absent, non-string, blank) is treated as
// undefined so the legacy per-category path stays untouched.
const resolveRaceTargetField = (
  body: Record<string, unknown>,
  field: string,
): string | undefined => {
  const requested = body[field];
  if (typeof requested !== "string") return undefined;
  const trimmed = requested.trim();
  return trimmed === "" ? undefined : trimmed;
};

const resolveDebugFlag = (body: Record<string, unknown>): boolean => {
  const requested = body[DEBUG_FIELD];
  if (requested === true) return true;
  if (typeof requested !== "string") return false;
  return ["1", "true", "yes", "on", "debug"].includes(requested.trim().toLowerCase());
};

const resolveTriggerDates = (body: Record<string, unknown>): RunDates => {
  const requested = body[RUN_DATE_FIELD];
  if (typeof requested === "string") {
    return parseRunDates(requested);
  }
  const now = new Date();
  return { runDate: getRunDateJst(now), runYmd: getRunYmdJst(now) };
};

const parseBody = async (request: Request): Promise<Record<string, unknown>> => {
  const text = await request.text();
  if (!text) {
    return {};
  }
  return JSON.parse(text) as Record<string, unknown>;
};

const handleTrigger = async (request: Request, env: Env): Promise<Response> => {
  if (!isAuthorized(request.headers.get("authorization"), env.TRIGGER_TOKEN)) {
    console.warn("[predict-worker] trigger unauthorized");
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }
  const body = await parseBody(request);
  const dates = resolveTriggerDates(body);
  const mode = resolveMode(body);
  const skipDedup = body[SKIP_DEDUP_FIELD] === true;
  const debug = resolveDebugFlag(body);
  const force = body[FORCE_FIELD] === true;
  const category = resolveCategory(body);
  const keibajoCode = resolveRaceTargetField(body, KEIBAJO_CODE_FIELD);
  const raceBango = resolveRaceTargetField(body, RACE_BANGO_FIELD);
  if (!hasRequiredPerRaceScope({ keibajoCode, raceBango })) {
    console.warn(
      `[predict-worker] trigger rejected day-scoped runDate=${dates.runDate} runYmd=${dates.runYmd} category=${
        category ?? "-"
      } mode=${mode} keibajo=${keibajoCode ?? "-"} race=${raceBango ?? "-"}`,
    );
    return Response.json(
      { error: PER_RACE_SCOPE_REQUIRED_ERROR, ok: false },
      { status: HTTP_BAD_REQUEST },
    );
  }
  if (debug) {
    console.log(
      `[predict-worker] trigger enqueue start runDate=${dates.runDate} runYmd=${dates.runYmd} category=${
        category ?? "-"
      } mode=${mode} keibajo=${keibajoCode} race=${raceBango} skipDedup=${skipDedup} debug=true`,
    );
  }
  const queued = await enqueuePredict({
    category,
    daysAhead: Number(env.PREDICT_DAYS_AHEAD),
    debug,
    env,
    keibajoCode,
    mode,
    raceBango,
    runDate: dates.runDate,
    runYmd: dates.runYmd,
    ...(skipDedup ? { skipDedup: true } : {}),
    ...(force ? { force: true } : {}),
  });
  console.log(
    `[predict-worker] trigger enqueue accepted runDate=${dates.runDate} runYmd=${dates.runYmd} category=${
      category ?? "-"
    } mode=${mode} queued=${queued.join(",")} debug=${debug}`,
  );
  return Response.json({ ok: true, queued, runDate: dates.runDate }, { status: HTTP_ACCEPTED });
};

const guardedTrigger = async (request: Request, env: Env): Promise<Response> => {
  try {
    return await handleTrigger(request, env);
  } catch (error) {
    return Response.json({ error: String(error), ok: false }, { status: HTTP_BAD_REQUEST });
  }
};

// True only for the internal event-driven per-race rescore route
// (POST /api/internal/rescore-race). The sync-realtime-data worker hits this
// path immediately after a horse-weight write to D1 so the race is re-scored
// with fresh weights without waiting for the 5-min coordinator cron poll.
export const isInternalRescoreRaceRequest = (method: string, pathname: string): boolean =>
  method === INTERNAL_RESCORE_RACE_METHOD && pathname === INTERNAL_RESCORE_RACE_PATH;

export const isInternalPredictionReadinessRequest = (method: string, pathname: string): boolean =>
  method === INTERNAL_MONITOR_METHOD && pathname === INTERNAL_READINESS_PATH;

export const isInternalDeliveryCanaryRequest = (method: string, pathname: string): boolean =>
  method === INTERNAL_MONITOR_METHOD && pathname === INTERNAL_CANARY_PATH;

export const isAdminStopContainersRequest = (method: string, pathname: string): boolean =>
  method === INTERNAL_RESCORE_RACE_METHOD && pathname === ADMIN_STOP_CONTAINERS_PATH;

export const isAdminCompleteFocusedFullRaceRequest = (method: string, pathname: string): boolean =>
  method === INTERNAL_RESCORE_RACE_METHOD && pathname === ADMIN_COMPLETE_FOCUSED_FULL_RACE_PATH;

export const isAdminRunFocusedFullRaceRequest = (method: string, pathname: string): boolean =>
  method === INTERNAL_RESCORE_RACE_METHOD && pathname === ADMIN_RUN_FOCUSED_FULL_RACE_PATH;

export const isAdminRunFocusedFullRaceDirectRequest = (method: string, pathname: string): boolean =>
  method === INTERNAL_RESCORE_RACE_METHOD && pathname === ADMIN_RUN_FOCUSED_FULL_RACE_DIRECT_PATH;

export const isAdminPrewarmDayBaseRequest = (method: string, pathname: string): boolean =>
  method === INTERNAL_RESCORE_RACE_METHOD && pathname === ADMIN_PREWARM_DAY_BASE_PATH;

export const isAdminPrewarmDayBaseStatusRequest = (method: string, pathname: string): boolean =>
  method === INTERNAL_MONITOR_METHOD && pathname === ADMIN_PREWARM_DAY_BASE_STATUS_PATH;

export const isAdminPickupDayBaseRequest = (method: string, pathname: string): boolean =>
  method === INTERNAL_RESCORE_RACE_METHOD && pathname === ADMIN_PICKUP_DAY_BASE_PATH;

export const isAdminMaterializeDayBaseRequest = (method: string, pathname: string): boolean =>
  method === INTERNAL_RESCORE_RACE_METHOD && pathname === ADMIN_MATERIALIZE_DAY_BASE_PATH;

export const isAdminPurgeUnusedPredictDoStateRequest = (
  method: string,
  pathname: string,
): boolean =>
  method === INTERNAL_RESCORE_RACE_METHOD && pathname === ADMIN_PURGE_UNUSED_PREDICT_DO_STATE_PATH;

const isValidRescoreCategory = (value: unknown): value is PredictCategory =>
  typeof value === "string" && VALID_CATEGORIES.has(value);

const isValidNonEmptyString = (value: unknown): value is string =>
  typeof value === "string" && value.trim().length > 0;

const isCanonicalHorseNumbers = (value: unknown): value is number[] =>
  Array.isArray(value) &&
  value.every(
    (horseNumber, index) =>
      Number.isInteger(horseNumber) &&
      Number(horseNumber) > 0 &&
      (index === 0 || Number(value[index - 1]) < Number(horseNumber)),
  );

const isValidRunYmd = (value: unknown): value is string =>
  typeof value === "string" && value.length === RUN_YMD_LENGTH && RUN_YMD_PATTERN.test(value);

const isFocusedFullTerminalStatus = (
  value: unknown,
): value is AdminCompleteFocusedFullRaceRequest["status"] =>
  value === "error" || value === "success";

const parseInternalRescoreRaceBody = (
  body: Record<string, unknown>,
): InternalRescoreRaceRequest | null => {
  const parsed = parseRaceScopedPredictBody(body);
  if (parsed === null) return null;
  const weightSnapshotCount = body[WEIGHT_SNAPSHOT_COUNT_FIELD];
  const weightSnapshotFetchedAt = body[WEIGHT_SNAPSHOT_FETCHED_AT_FIELD];
  const weightSnapshotHash = body[WEIGHT_SNAPSHOT_HASH_FIELD];
  const activeHorseNumbers = body.activeHorseNumbers;
  const excludedHorseNumbers = body.excludedHorseNumbers;
  const entrySnapshotFetchedAt = body.entrySnapshotFetchedAt;
  const entrySnapshotHash = body.entrySnapshotHash;
  if (!Number.isInteger(weightSnapshotCount) || Number(weightSnapshotCount) <= 0) return null;
  if (
    !isValidNonEmptyString(weightSnapshotFetchedAt) ||
    !Number.isFinite(Date.parse(weightSnapshotFetchedAt))
  )
    return null;
  if (typeof weightSnapshotHash !== "string" || !SHA256_HEX_PATTERN.test(weightSnapshotHash))
    return null;
  const hasEntrySnapshot =
    activeHorseNumbers !== undefined ||
    excludedHorseNumbers !== undefined ||
    entrySnapshotFetchedAt !== undefined ||
    entrySnapshotHash !== undefined;
  if (hasEntrySnapshot) {
    if (!isCanonicalHorseNumbers(activeHorseNumbers) || activeHorseNumbers.length === 0)
      return null;
    if (!isCanonicalHorseNumbers(excludedHorseNumbers)) return null;
    if (activeHorseNumbers.some((horseNumber) => excludedHorseNumbers.includes(horseNumber)))
      return null;
    if (activeHorseNumbers.length > Number(weightSnapshotCount)) return null;
    if (entrySnapshotFetchedAt !== weightSnapshotFetchedAt) return null;
    if (typeof entrySnapshotHash !== "string" || !SHA256_HEX_PATTERN.test(entrySnapshotHash))
      return null;
  }
  return {
    ...parsed,
    ...(hasEntrySnapshot && isCanonicalHorseNumbers(activeHorseNumbers)
      ? { activeHorseNumbers }
      : {}),
    ...(hasEntrySnapshot && isCanonicalHorseNumbers(excludedHorseNumbers)
      ? { excludedHorseNumbers }
      : {}),
    ...(hasEntrySnapshot && typeof entrySnapshotFetchedAt === "string"
      ? { entrySnapshotFetchedAt }
      : {}),
    ...(hasEntrySnapshot && typeof entrySnapshotHash === "string" ? { entrySnapshotHash } : {}),
    weightSnapshotCount: Number(weightSnapshotCount),
    weightSnapshotFetchedAt,
    weightSnapshotHash,
  };
};

const parseRaceScopedPredictBody = (
  body: Record<string, unknown>,
): RaceScopedPredictRequest | null => {
  const category = body[CATEGORY_FIELD];
  const keibajoCode = body[KEIBAJO_CODE_FIELD];
  const raceBango = body[RACE_BANGO_FIELD];
  const runYmd = body[RUN_YMD_FIELD];
  if (!isValidRescoreCategory(category)) return null;
  if (!isValidNonEmptyString(keibajoCode)) return null;
  if (!isValidNonEmptyString(raceBango)) return null;
  if (!isValidRunYmd(runYmd)) return null;
  const raceTarget = normalizePerRaceScope({ keibajoCode, raceBango });
  if (raceTarget === null) return null;
  const raceStartAtJst = body[RACE_START_AT_JST_FIELD];
  if (
    raceStartAtJst !== undefined &&
    (!isValidNonEmptyString(raceStartAtJst) || !Number.isFinite(Date.parse(raceStartAtJst)))
  )
    return null;
  return {
    category,
    ...(resolveDebugFlag(body) ? { debug: true } : {}),
    ...(body[FORCE_FIELD] === true ? { force: true } : {}),
    ...raceTarget,
    runYmd,
    ...(typeof raceStartAtJst === "string" ? { raceStartAtJst } : {}),
  };
};

const parseAdminPrewarmDayBaseBody = (
  body: Record<string, unknown>,
  fallbackRunYmd: string,
): AdminPrewarmDayBaseRequest | null => {
  const requestedRunYmd = body[RUN_YMD_FIELD];
  const runYmd = requestedRunYmd === undefined ? fallbackRunYmd : requestedRunYmd;
  if (!isValidRunYmd(runYmd)) return null;
  const category = body[CATEGORY_FIELD];
  const force = body[FORCE_FIELD];
  if (force !== undefined && typeof force !== "boolean") return null;
  const generatePredictionsAfterHit = body.generatePredictionsAfterHit;
  if (generatePredictionsAfterHit !== undefined && typeof generatePredictionsAfterHit !== "boolean")
    return null;
  const generationFlag =
    generatePredictionsAfterHit === true ? { generatePredictionsAfterHit: true } : {};
  const forceFlag = force === true ? { force: true } : {};
  if (category === undefined) {
    return generatePredictionsAfterHit === true || force === true ? null : { runYmd };
  }
  if (!isValidRescoreCategory(category)) return null;
  return { category, ...forceFlag, ...generationFlag, runYmd };
};

const parseAdminCompleteFocusedFullRaceBody = (
  body: Record<string, unknown>,
): AdminCompleteFocusedFullRaceRequest | null => {
  const parsed = parseRaceScopedPredictBody(body);
  if (!parsed || !isFocusedFullTerminalStatus(body.status)) return null;
  return { ...parsed, status: body.status };
};

const buildRunDateFromYmd = (runYmd: string): string =>
  [
    runYmd.slice(0, RUN_YMD_YEAR_END),
    runYmd.slice(RUN_YMD_YEAR_END, RUN_YMD_MONTH_END),
    runYmd.slice(RUN_YMD_MONTH_END, RUN_YMD_LENGTH),
  ].join(RUN_DATE_SEPARATOR);

const describeRaceRequest = (body: RaceScopedPredictRequest): string =>
  `category=${body.category} runYmd=${body.runYmd} keibajo=${body.keibajoCode} race=${body.raceBango}`;

const resolveRescoreRaceStartAtJst = async (
  env: Env,
  body: InternalRescoreRaceRequest,
): Promise<string> => {
  if (body.raceStartAtJst !== undefined) return body.raceStartAtJst;
  const source = body.category === "jra" ? "jra" : "nar";
  const result = await env.REALTIME_DB.prepare(
    `select race_start_at_jst
       from realtime_race_sources
      where source = ?1 and kaisai_nen = ?2 and kaisai_tsukihi = ?3
        and keibajo_code = ?4 and race_bango = ?5
      limit 1`,
  )
    .bind(
      source,
      body.runYmd.slice(0, RUN_YMD_YEAR_END),
      body.runYmd.slice(RUN_YMD_YEAR_END),
      body.keibajoCode,
      body.raceBango,
    )
    .all<RescoreRaceStartRow>();
  const raceStartAtJst = result.results[0]?.race_start_at_jst;
  if (raceStartAtJst === undefined || !Number.isFinite(Date.parse(raceStartAtJst))) {
    throw new Error(`Missing valid race start: ${describeRaceRequest(body)}`);
  }
  return raceStartAtJst;
};

const sendRescoreRaceMessage = async (
  env: Env,
  body: InternalRescoreRaceRequest,
): Promise<void> => {
  const runDate = buildRunDateFromYmd(body.runYmd);
  const queue =
    env.WEIGHT_RESCORE_QUEUE === undefined ? env.PREDICT_QUEUE : env.WEIGHT_RESCORE_QUEUE;
  await queue.send({
    category: body.category,
    daysAhead: RESCORE_DAYS_AHEAD,
    ...(body.debug ? { debug: true } : {}),
    keibajoCode: body.keibajoCode,
    mode: RESCORE_MODE,
    raceBango: body.raceBango,
    runDate,
    runDateIso: runDate,
    runYmd: body.runYmd,
    raceStartAtJst: body.raceStartAtJst,
    activeHorseNumbers: body.activeHorseNumbers,
    excludedHorseNumbers: body.excludedHorseNumbers,
    entrySnapshotFetchedAt: body.entrySnapshotFetchedAt,
    entrySnapshotHash: body.entrySnapshotHash,
    weightSnapshotCount: body.weightSnapshotCount,
    weightSnapshotFetchedAt: body.weightSnapshotFetchedAt,
    weightSnapshotHash: body.weightSnapshotHash,
  } satisfies PredictQueueMessage);
};

const handleInternalRescoreRace = async (request: Request, env: Env): Promise<Response> => {
  if (!isAuthorized(request.headers.get("authorization"), env.TRIGGER_TOKEN)) {
    console.warn("[predict-worker] internal-rescore unauthorized");
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }
  if (env.RESCORE_ENABLED !== RESCORE_ENABLED_FLAG) {
    console.log("[predict-worker] internal-rescore skipped rescoreEnabled=false");
    return Response.json({ claimed: false, ok: true, rescoreEnabled: false }, { status: HTTP_OK });
  }
  const raw = await parseBody(request);
  const parsed = parseInternalRescoreRaceBody(raw);
  if (!parsed) {
    console.warn("[predict-worker] internal-rescore invalid request");
    return Response.json({ error: "invalid request", ok: false }, { status: HTTP_BAD_REQUEST });
  }
  if (parsed.debug) {
    console.log(`[predict-worker] internal-rescore claim start ${describeRaceRequest(parsed)}`);
  }
  const raceStartAtJst = await resolveRescoreRaceStartAtJst(env, parsed);
  const claimId = crypto.randomUUID();
  const claimParams = {
    category: parsed.category,
    claimId,
    env,
    keibajoCode: parsed.keibajoCode,
    raceBango: parsed.raceBango,
    runYmd: parsed.runYmd,
    weightSnapshotCount: parsed.weightSnapshotCount,
    weightSnapshotFetchedAt: parsed.weightSnapshotFetchedAt,
    weightSnapshotHash: parsed.weightSnapshotHash,
  };
  const claim = await claimRescoreRace(claimParams);
  if (parsed.debug) {
    console.log(
      `[predict-worker] internal-rescore claim result ${describeRaceRequest(parsed)} proceed=${
        claim.proceed
      } state=${claim.state ?? "-"}`,
    );
  }
  if (!claim.proceed) {
    return Response.json({ claimed: false, ok: true }, { status: HTTP_OK });
  }
  try {
    // Warm the Neon compute endpoint immediately before the weight-rescore
    // message reaches the Container. This is best-effort and fail-closed
    // inside warmNeon, so a transient warm failure never suppresses a valid
    // prediction request.
    if (env.NEON_DATABASE_URL !== undefined) {
      await warmNeon(env.NEON_DATABASE_URL);
    }
    await sendRescoreRaceMessage(env, { ...parsed, raceStartAtJst });
  } catch (error) {
    try {
      await releaseRescoreRaceClaim(claimParams);
    } catch (releaseError) {
      console.error(
        `[predict-worker] internal-rescore claim release failed ${describeRaceRequest(parsed)}:`,
        String(releaseError),
      );
    }
    throw error;
  }
  console.log(`[predict-worker] internal-rescore enqueued ${describeRaceRequest(parsed)}`);
  return Response.json({ claimed: true, ok: true }, { status: HTTP_ACCEPTED });
};

const guardedInternalRescoreRace = async (request: Request, env: Env): Promise<Response> => {
  try {
    return await handleInternalRescoreRace(request, env);
  } catch (error) {
    return Response.json({ error: String(error), ok: false }, { status: HTTP_BAD_REQUEST });
  }
};

const parseStopContainerTarget = (name: unknown): AdminContainerStopTarget | null => {
  if (typeof name !== "string") return null;
  if (isAllowedContainerDoName(name, "legacy")) return { name, role: "legacy" };
  return isAllowedContainerDoName(name, "race-chain") ? { name, role: "race-chain" } : null;
};

const parseStopContainerTargets = (
  body: Record<string, unknown>,
): AdminContainerStopTarget[] | null => {
  const names = body.names;
  if (!Array.isArray(names)) return null;
  if (names.length === 0 || names.length > MAX_ADMIN_STOP_NAMES) return null;
  const targets = names.map(parseStopContainerTarget);
  return targets.every((target): target is AdminContainerStopTarget => target !== null)
    ? targets
    : null;
};

const parseStopContainerOverrideActive = (body: Record<string, unknown>): boolean | null => {
  const overrideActive = body.overrideActive;
  if (overrideActive === undefined) return false;
  return typeof overrideActive === "boolean" ? overrideActive : null;
};

const handleAdminStopContainers = async (request: Request, env: Env): Promise<Response> => {
  if (!isAuthorized(request.headers.get("authorization"), env.TRIGGER_TOKEN)) {
    console.warn("[predict-worker] admin-stop unauthorized");
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }
  const body = await parseBody(request);
  const targets = parseStopContainerTargets(body);
  if (!targets) {
    console.warn("[predict-worker] admin-stop invalid names");
    return Response.json({ error: "invalid names", ok: false }, { status: HTTP_BAD_REQUEST });
  }
  const names = targets.map((target) => target.name);
  const overrideActive = parseStopContainerOverrideActive(body);
  if (overrideActive === null) {
    console.warn("[predict-worker] admin-stop invalid overrideActive");
    return Response.json(
      { error: "invalid overrideActive", ok: false },
      { status: HTTP_BAD_REQUEST },
    );
  }
  console.warn(
    `[predict-worker] admin-stop requested count=${names.length} names=${names.join(",")} overrideActive=${overrideActive}`,
  );
  if (env.CONTAINER_CONTROL_QUEUE === undefined) {
    return Response.json(
      { error: "container control queue unavailable", ok: false },
      { status: HTTP_SERVICE_UNAVAILABLE },
    );
  }
  const requestedAt = new Date().toISOString();
  const controlQueue = env.CONTAINER_CONTROL_QUEUE;
  await Promise.all(
    targets.map((target) =>
      controlQueue.send({
        ...(overrideActive ? { force: true } : {}),
        name: target.name,
        requestedAt,
        role: target.role,
        type: "container-stop",
      } satisfies ContainerControlMessage),
    ),
  );
  return Response.json({ names, ok: true, queued: names.length }, { status: HTTP_ACCEPTED });
};

const guardedAdminStopContainers = async (request: Request, env: Env): Promise<Response> => {
  try {
    return await handleAdminStopContainers(request, env);
  } catch (error) {
    return Response.json({ error: String(error), ok: false }, { status: HTTP_BAD_REQUEST });
  }
};

const purgePredictDoStateById = async (id: string, env: Env): Promise<Response> => {
  const namespace = env.FINISH_POSITION_PREDICT_CONTAINER;
  const durableObjectId = namespace.idFromString(id);
  return namespace.get(durableObjectId).fetch(
    new Request(`http://predict-container-do${PREDICT_DO_INTERNAL_PURGE_PATH}`, {
      headers: { authorization: `Bearer ${env.TRIGGER_TOKEN}` },
      method: "POST",
    }),
  );
};

const guardedAdminPurgeUnusedPredictDoState = async (
  request: Request,
  env: Env,
): Promise<Response> => {
  try {
    return await handlePredictDoStatePurge(request, {
      purgeId: (id) => purgePredictDoStateById(id, env),
      resolveIdFromName: (name) =>
        env.FINISH_POSITION_PREDICT_CONTAINER.idFromName(name).toString(),
      triggerToken: env.TRIGGER_TOKEN,
    });
  } catch (error) {
    return Response.json({ error: String(error), ok: false }, { status: HTTP_BAD_REQUEST });
  }
};

const handleAdminRunFocusedFullRace = async (request: Request, env: Env): Promise<Response> => {
  if (!isAuthorized(request.headers.get("authorization"), env.TRIGGER_TOKEN)) {
    console.warn("[predict-worker] admin-run-focused-full unauthorized");
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }
  const raw = await parseBody(request);
  const parsed = parseRaceScopedPredictBody(raw);
  if (!parsed) {
    console.warn("[predict-worker] admin-run-focused-full invalid request");
    return Response.json({ error: "invalid request", ok: false }, { status: HTTP_BAD_REQUEST });
  }
  const runDate = buildRunDateFromYmd(parsed.runYmd);
  await enqueuePredict({
    category: parsed.category,
    daysAhead: RESCORE_DAYS_AHEAD,
    ...(parsed.debug === true ? { debug: true } : {}),
    env,
    ...(parsed.force === true ? { force: true } : {}),
    keibajoCode: parsed.keibajoCode,
    mode: DEFAULT_MODE,
    raceBango: parsed.raceBango,
    runDate,
    runYmd: parsed.runYmd,
    skipDedup: true,
  });
  return Response.json({ ok: true, queued: true, ...parsed }, { status: HTTP_ACCEPTED });
};

const guardedAdminRunFocusedFullRace = async (request: Request, env: Env): Promise<Response> => {
  try {
    return await handleAdminRunFocusedFullRace(request, env);
  } catch (error) {
    return Response.json({ error: String(error), ok: false }, { status: HTTP_BAD_REQUEST });
  }
};

const handleAdminRunFocusedFullRaceDirect = async (
  request: Request,
  env: Env,
): Promise<Response> => {
  if (!isAuthorized(request.headers.get("authorization"), env.TRIGGER_TOKEN)) {
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }
  const parsed = parseRaceScopedPredictBody(await parseBody(request));
  if (parsed === null)
    return Response.json({ error: "invalid request", ok: false }, { status: HTTP_BAD_REQUEST });
  const doName = resolvePredictDoName({
    category: parsed.category,
    env,
    keibajoCode: parsed.keibajoCode,
    raceBango: parsed.raceBango,
  });
  const body = {
    category: parsed.category,
    daysAhead: RESCORE_DAYS_AHEAD,
    force: true,
    keibajoCode: parsed.keibajoCode,
    mode: DEFAULT_MODE,
    raceBango: parsed.raceBango,
    runDate: parsed.runYmd,
    runDateIso: buildRunDateFromYmd(parsed.runYmd),
    runYmd: parsed.runYmd,
    skipDedup: true,
    deliveryTrackingId: `preweight:${parsed.runYmd}:${parsed.category}:${parsed.keibajoCode}:${parsed.raceBango}`,
  } satisfies PredictQueueMessage;
  const workKey = `focused-full:${body.runYmd}:${body.category}:${body.keibajoCode}:${body.raceBango}:direct`;
  const watchPayload = {
    body,
    doName,
    role: "legacy",
    watchId: `${workKey}:direct-${crypto.randomUUID()}`,
    workKey,
  } satisfies FocusedFullWatchPayload;
  const searchParams = new URLSearchParams({
    category: body.category,
    daysAhead: String(body.daysAhead),
    force: "true",
    keibajoCode: body.keibajoCode,
    mode: body.mode,
    raceBango: body.raceBango,
    runDate: body.runDate,
    runYmd: body.runYmd,
  });
  const doId = env.FINISH_POSITION_PREDICT_CONTAINER.idFromName(doName);
  await recordPreweightGenerationStarted(env, body, new Date());
  const response = await env.FINISH_POSITION_PREDICT_CONTAINER.get(doId).fetch(
    new Request(`http://predict-container-do/predict?${searchParams.toString()}`, {
      headers: { [WATCH_REQUEST_HEADER]: JSON.stringify(watchPayload) },
    }),
  );
  const responseBody = response.ok ? undefined : await response.text();
  return Response.json(
    { error: responseBody, ok: response.ok, status: response.status, queued: true },
    { status: HTTP_ACCEPTED },
  );
};

const guardedAdminRunFocusedFullRaceDirect = async (
  request: Request,
  env: Env,
): Promise<Response> => {
  try {
    return await handleAdminRunFocusedFullRaceDirect(request, env);
  } catch (error) {
    return Response.json({ error: String(error), ok: false }, { status: HTTP_BAD_REQUEST });
  }
};

const handleAdminPrewarmDayBase = async (request: Request, env: Env): Promise<Response> => {
  if (!isAuthorized(request.headers.get("authorization"), env.TRIGGER_TOKEN)) {
    console.warn("[predict-worker] admin-prewarm-day-base unauthorized");
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }
  const parsed = parseAdminPrewarmDayBaseBody(await parseBody(request), getRunYmdJst(new Date()));
  if (parsed === null) {
    console.warn("[predict-worker] admin-prewarm-day-base invalid request");
    return Response.json({ error: "invalid request", ok: false }, { status: HTTP_BAD_REQUEST });
  }
  // The object key and freshness watermark are both scoped to exactly
  // runYmd. Building runYmd+1/+2 into that same object makes today's urgent
  // pre-weight pass needlessly expensive and lets future-day changes escape
  // the target-day watermark. Admin/source-event prewarms therefore build one
  // exact day; scheduled catch-up can still enumerate and dispatch each day
  // independently.
  const daysAhead = 0;
  console.warn(
    `[predict-worker] admin-prewarm-day-base start runYmd=${parsed.runYmd} category=${parsed.category ?? "all"}`,
  );
  if (parsed.category !== undefined) {
    const outcome = await prewarmCategoryWithOutcome({
      category: parsed.category,
      daysAhead,
      env,
      generationId: crypto.randomUUID(),
      ...(parsed.force === true ? { force: true } : {}),
      ...(parsed.generatePredictionsAfterHit === true ? { generatePredictionsAfterHit: true } : {}),
      runYmd: parsed.runYmd,
    });
    const accepted = outcome !== "failed";
    const status =
      outcome === "landed" ? HTTP_OK : accepted ? HTTP_ACCEPTED : HTTP_SERVICE_UNAVAILABLE;
    return Response.json(
      {
        accepted,
        category: parsed.category,
        ok: accepted,
        outcome,
        queued: false,
        runYmd: parsed.runYmd,
      },
      { status },
    );
  }
  const queued = await runDayBasePrewarm({ daysAhead, env, runYmd: parsed.runYmd });
  return Response.json(
    {
      category: parsed.category ?? "all",
      ok: queued,
      queued,
      runYmd: parsed.runYmd,
    },
    { status: HTTP_ACCEPTED },
  );
};

const guardedAdminPrewarmDayBase = async (request: Request, env: Env): Promise<Response> => {
  try {
    return await handleAdminPrewarmDayBase(request, env);
  } catch (error) {
    return Response.json({ error: String(error), ok: false }, { status: HTTP_BAD_REQUEST });
  }
};

const handleAdminPrewarmDayBaseStatus = async (request: Request, env: Env): Promise<Response> => {
  if (!isAuthorized(request.headers.get("authorization"), env.TRIGGER_TOKEN)) {
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }
  const searchParams = new URL(request.url).searchParams;
  const category = searchParams.get(CATEGORY_FIELD);
  const runYmd = searchParams.get(RUN_YMD_FIELD);
  if (!isValidRescoreCategory(category) || !isValidRunYmd(runYmd)) {
    return Response.json({ error: "invalid request", ok: false }, { status: HTTP_BAD_REQUEST });
  }
  const doName = resolvePredictDoName({ category, env });
  const doId = env.FINISH_POSITION_PREDICT_CONTAINER.idFromName(doName);
  const statusSearchParams = new URLSearchParams({ category, runDate: runYmd });
  return env.FINISH_POSITION_PREDICT_CONTAINER.get(doId).fetch(
    new Request(
      `http://predict-container-do/prewarm-day-base-status?${statusSearchParams.toString()}`,
    ),
  );
};

const guardedAdminPrewarmDayBaseStatus = async (request: Request, env: Env): Promise<Response> => {
  try {
    return await handleAdminPrewarmDayBaseStatus(request, env);
  } catch (error) {
    return Response.json({ error: String(error), ok: false }, { status: HTTP_SERVICE_UNAVAILABLE });
  }
};

const handleAdminPickupDayBase = async (request: Request, env: Env): Promise<Response> => {
  if (!isAuthorized(request.headers.get("authorization"), env.TRIGGER_TOKEN)) {
    console.warn("[predict-worker] admin-pickup-day-base unauthorized");
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }
  const parsed = parseAdminPrewarmDayBaseBody(await parseBody(request), getRunYmdJst(new Date()));
  if (parsed === null || parsed.category === undefined) {
    console.warn("[predict-worker] admin-pickup-day-base invalid request");
    return Response.json({ error: "invalid request", ok: false }, { status: HTTP_BAD_REQUEST });
  }
  console.warn(
    `[predict-worker] admin-pickup-day-base start runYmd=${parsed.runYmd} category=${parsed.category}`,
  );
  const existingReadiness = await getFocusedFullDayBaseReadiness({
    category: parsed.category,
    env,
    runYmd: parsed.runYmd,
  }).catch(() => ({ ready: false, reason: "readiness-error" }));
  const pickupFound = existingReadiness.ready
    ? true
    : await pickUpPrewarmDayBase({
        category: parsed.category,
        env,
        runYmd: parsed.runYmd,
      });
  const readiness = existingReadiness.ready
    ? existingReadiness
    : pickupFound
      ? await getFocusedFullDayBaseReadiness({
          category: parsed.category,
          env,
          runYmd: parsed.runYmd,
        })
      : { ready: false, reason: "pickup-missing" };
  const pickedUp = readiness.ready;
  const landed = pickedUp;
  const racesEnqueued = landed
    ? await completeLandedDayBase({
        category: parsed.category,
        env,
        generatePredictionsAfterHit: parsed.generatePredictionsAfterHit === true,
        runYmd: parsed.runYmd,
      })
    : 0;
  return Response.json({
    category: parsed.category,
    ok: landed,
    pickedUp,
    readiness: readiness.reason,
    racesEnqueued,
    runYmd: parsed.runYmd,
  });
};

const guardedAdminPickupDayBase = async (request: Request, env: Env): Promise<Response> => {
  try {
    return await handleAdminPickupDayBase(request, env);
  } catch (error) {
    return Response.json({ error: String(error), ok: false }, { status: HTTP_SERVICE_UNAVAILABLE });
  }
};

const handleAdminMaterializeDayBase = async (request: Request, env: Env): Promise<Response> => {
  if (!isAuthorized(request.headers.get("authorization"), env.TRIGGER_TOKEN)) {
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }
  const parsed = parseAdminPrewarmDayBaseBody(await parseBody(request), getRunYmdJst(new Date()));
  if (parsed === null || parsed.category === undefined) {
    return Response.json({ error: "invalid request", ok: false }, { status: HTTP_BAD_REQUEST });
  }
  const result = await materializeDayBasePerRaceCache({
    category: parsed.category,
    env,
    runYmd: parsed.runYmd,
  });
  const ok = result.status === "materialized";
  return Response.json(
    { category: parsed.category, ok, result, runYmd: parsed.runYmd },
    {
      status: ok ? HTTP_OK : HTTP_SERVICE_UNAVAILABLE,
    },
  );
};

const guardedAdminMaterializeDayBase = async (request: Request, env: Env): Promise<Response> => {
  try {
    return await handleAdminMaterializeDayBase(request, env);
  } catch (error) {
    return Response.json({ error: String(error), ok: false }, { status: HTTP_BAD_REQUEST });
  }
};

const handleAdminCompleteFocusedFullRace = async (
  request: Request,
  env: Env,
): Promise<Response> => {
  if (!isAuthorized(request.headers.get("authorization"), env.TRIGGER_TOKEN)) {
    console.warn("[predict-worker] admin-complete-focused-full unauthorized");
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }
  const raw = await parseBody(request);
  const parsed = parseAdminCompleteFocusedFullRaceBody(raw);
  if (!parsed) {
    console.warn("[predict-worker] admin-complete-focused-full invalid request");
    return Response.json({ error: "invalid request", ok: false }, { status: HTTP_BAD_REQUEST });
  }
  console.log(
    `[predict-worker] admin-complete-focused-full start ${describeRaceRequest(parsed)} status=${parsed.status}`,
  );
  await completeFocusedFullRace({ env, ...parsed });
  console.log(
    `[predict-worker] admin-complete-focused-full completed ${describeRaceRequest(
      parsed,
    )} status=${parsed.status}`,
  );
  return Response.json({ ok: true });
};

const guardedAdminCompleteFocusedFullRace = async (
  request: Request,
  env: Env,
): Promise<Response> => {
  try {
    return await handleAdminCompleteFocusedFullRace(request, env);
  } catch (error) {
    return Response.json({ error: String(error), ok: false }, { status: HTTP_BAD_REQUEST });
  }
};

const isMonitorAuthorized = (request: Request, env: Env): boolean =>
  isAuthorized(request.headers.get("authorization"), env.TRIGGER_TOKEN);

const handlePredictionReadiness = async (request: Request, env: Env): Promise<Response> => {
  if (!isMonitorAuthorized(request, env)) {
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }
  const now = new Date();
  const requestedRunYmd = new URL(request.url).searchParams.get(RUN_YMD_FIELD);
  const runYmd = requestedRunYmd === null ? getRunYmdJst(now) : requestedRunYmd;
  if (!isValidRunYmd(runYmd)) {
    return Response.json({ error: "invalid runYmd", ok: false }, { status: HTTP_BAD_REQUEST });
  }
  const readiness = await getPredictionReadiness({ env, now, runYmd });
  return Response.json(readiness);
};

const handleDeliveryCanaries = async (request: Request, env: Env): Promise<Response> => {
  if (!isMonitorAuthorized(request, env)) {
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }
  return Response.json({
    canaries: await listDeliveryCanaries(env),
    checkedAt: new Date().toISOString(),
  });
};

export const handleFetch = async (request: Request, env: Env): Promise<Response> => {
  const url = new URL(request.url);
  if (isTriggerRequest(request.method, url.pathname)) {
    return guardedTrigger(request, env);
  }
  if (isAdminStopContainersRequest(request.method, url.pathname)) {
    return guardedAdminStopContainers(request, env);
  }
  if (isAdminPurgeUnusedPredictDoStateRequest(request.method, url.pathname)) {
    return guardedAdminPurgeUnusedPredictDoState(request, env);
  }
  if (isAdminCompleteFocusedFullRaceRequest(request.method, url.pathname)) {
    return guardedAdminCompleteFocusedFullRace(request, env);
  }
  if (isAdminRunFocusedFullRaceRequest(request.method, url.pathname)) {
    return guardedAdminRunFocusedFullRace(request, env);
  }
  if (isAdminRunFocusedFullRaceDirectRequest(request.method, url.pathname)) {
    return guardedAdminRunFocusedFullRaceDirect(request, env);
  }
  if (isAdminPrewarmDayBaseRequest(request.method, url.pathname)) {
    return guardedAdminPrewarmDayBase(request, env);
  }
  if (isAdminPrewarmDayBaseStatusRequest(request.method, url.pathname)) {
    return guardedAdminPrewarmDayBaseStatus(request, env);
  }
  if (isAdminPickupDayBaseRequest(request.method, url.pathname)) {
    return guardedAdminPickupDayBase(request, env);
  }
  if (isAdminMaterializeDayBaseRequest(request.method, url.pathname)) {
    return guardedAdminMaterializeDayBase(request, env);
  }
  if (isInternalRescoreRaceRequest(request.method, url.pathname)) {
    return guardedInternalRescoreRace(request, env);
  }
  if (isInternalPredictionReadinessRequest(request.method, url.pathname)) {
    return handlePredictionReadiness(request, env);
  }
  if (isInternalDeliveryCanaryRequest(request.method, url.pathname)) {
    return handleDeliveryCanaries(request, env);
  }
  return healthResponse();
};

export const handleScheduled = async (event: ScheduledEvent, env: Env): Promise<void> => {
  if (shouldRunDeliveryCanaryCron(event.cron)) {
    await enqueueDeliveryCanary(env, new Date(event.scheduledTime));
    return;
  }
  if (shouldRunWarmCron(event.cron)) {
    await warmNeon(env.NEON_DATABASE_URL);
    return;
  }
  if (shouldRunCoordinatorCron(event.cron)) {
    // Legacy/manual compatibility only. No production cron is registered for
    // this path; normal second-pass generation is weight-event driven.
    await runRaceCoordinatorTick({
      env,
      leadMinutes: DEFAULT_RESCORE_LEAD_MINUTES,
      now: new Date(event.scheduledTime),
    });
    return;
  }
  if (shouldRunCoverageSelfHealCron(event.cron)) {
    // Per-race coverage self-healing + pre-race readiness scan (doc §4.3,
    // docs/cf-only-serving-architecture.md): the direct functional
    // replacement for race-prediction-guard.sh's day-wide COUNT check.
    // Same tick also enqueues mode=full skipDedup for incomplete races in
    // (now, now+PRE_RACE_LEAD_MINUTES] so predictions exist before post, not
    // only via the post-grace heal path. Post-race path still re-enqueues
    // races whose post time is >15 min past and still missing rows, respecting
    // focused-full DO claim/heartbeat/staleness -- never a day-wide re-run.
    // See coverage-self-heal.ts.
    await runCoverageSelfHeal({ env, now: new Date(event.scheduledTime) });
    return;
  }
  if (shouldRunRunningStyleKickMorningGapCron(event.cron)) {
    // Fills the JST 00:00-08:59 gap sync-realtime-data's own native
    // running-style crons do not cover for TODAY (see running-style-kick.ts
    // module doc) -- the Cloudflare replacement for
    // scripts/launchd/race-prediction-guard.sh's JST 0-9 hourly RS kick band.
    await runRunningStyleKickMorningGap({ env, now: new Date(event.scheduledTime) });
    return;
  }
  if (shouldRunRunningStyleKickTomorrowPrewarmCron(event.cron)) {
    // Redundant re-attempts for TOMORROW's prewarm at JST 22:00/23:00,
    // matching the guard's own 21/22/23 hourly tomorrow band (see
    // running-style-kick.ts module doc).
    await runRunningStyleKickTomorrowPrewarm({ env, now: new Date(event.scheduledTime) });
    return;
  }
  if (shouldRunCornerFeaturesRefreshCron(event.cron)) {
    // §4.4 independent Neon-side refresh of race_entry_corner_features (docs/
    // probes/corner-features-settlement-backfill-heal-2026-07-17.md): fires
    // twice daily (morning pre-race populate, evening settlement catch-up --
    // see CORNER_FEATURES_REFRESH_CRON_MORNING/_EVENING). lookbackDays widens
    // the upsert window backward so a day whose settlement columns were still
    // NULL on this cron's last visit gets swept up again instead of being
    // permanently stuck once it ages past [runYmd, runYmd+daysAhead] -- the
    // exact failure mode this cron's prior five days of not being wired at
    // all had already produced. Idempotent upsert-only, matching every other
    // maintenance cron in this file.
    const scheduledAt = new Date(event.scheduledTime);
    await refreshCornerFeatures({
      daysAhead: Number(env.PREDICT_DAYS_AHEAD),
      env,
      lookbackDays: Number(env.CORNER_FEATURES_LOOKBACK_DAYS ?? CORNER_FEATURES_NO_LOOKBACK_DAYS),
      runYmd: getRunYmdJst(scheduledAt),
    });
    return;
  }
  if (shouldRunFeatureBuildCron(event.cron)) {
    const scheduledAt = new Date(event.scheduledTime);
    const runYmd = getRunYmdJst(scheduledAt);
    const daysAhead = Number(env.PREDICT_DAYS_AHEAD);
    // Warms the per-category, per-day "day-base" feature parquet cache in the
    // Container (see day-base-prewarm.ts) so the day-stable feature layers are
    // built once per day instead of once per race. Production full per-race
    // runs are still triggered by sync-realtime-data after running-style
    // completes via POST /run with skipDedup=true; this cron only pre-builds
    // the cache those runs then reuse.
    await runDayBasePrewarm({ daysAhead, env, runYmd });
    return;
  }
  if (shouldRunRescoreCron(event.cron)) {
    // Day-scoped category-wide rescore is permanently disabled under the
    // per-race-only production policy. The schedule itself remains commented
    // out in wrangler.jsonc; this branch is a hard no-op belt if it is ever
    // re-enabled by mistake. Per-race freshness uses the coordinator cron +
    // POST /run / internal rescore-race paths instead.
    console.warn(
      `[predict-worker] refusing day-scoped rescore cron=${event.cron}; per-race scope required`,
    );
    return;
  }
  if (!shouldRunPredictCron(event.cron)) {
    return;
  }
  // Legacy monolithic predict cron (PREDICT_CRON) is absent from wrangler and
  // must never enqueue day-scoped generation even if re-added to triggers.
  console.warn(
    `[predict-worker] refusing day-scoped predict cron=${event.cron}; per-race scope required`,
  );
};

// A single Worker script consumes both the primary predict queue and its
// dead-letter queue (two consumer entries in wrangler.jsonc, both routed to
// this one `queue()` export); MessageBatch.queue names which queue the batch
// came from, so route accordingly instead of running the two independent
// consumer implementations against the wrong batch shape.
export const handleQueueBatch = async (
  batch: MessageBatch<PredictQueueBody | ContainerControlMessage>,
  env: Env,
): Promise<void> => {
  if (batch.queue === CONTAINER_CONTROL_QUEUE_NAME) {
    for (const message of batch.messages) {
      if (!isContainerControlQueueMessage(message)) continue;
      try {
        await consumeContainerStop(env, message.body);
        message.ack();
      } catch (error) {
        console.error("[container-control] stop failed:", String(error));
        message.retry({ delaySeconds: 30 });
      }
    }
    return;
  }
  if (batch.queue === DLQ_QUEUE_NAME) {
    await handleDlqQueue(batch as MessageBatch<PredictQueueBody | ContainerControlMessage>, env);
    return;
  }
  await handleQueue(batch as MessageBatch<PredictQueueBody>, env);
};

export default {
  async fetch(request: Request, env: Env, _ctx: ExecutionContext): Promise<Response> {
    return handleFetch(request, env);
  },
  async scheduled(event: ScheduledEvent, env: Env): Promise<void> {
    await handleScheduled(event, env);
  },
  async queue(
    batch: MessageBatch<PredictQueueBody | ContainerControlMessage>,
    env: Env,
  ): Promise<void> {
    await handleQueueBatch(batch, env);
  },
};
