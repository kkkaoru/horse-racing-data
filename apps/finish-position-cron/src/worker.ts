// Run with bun. Fetch (health + on-demand trigger) + scheduled (cron -> container) + queue handlers.

import { FinishPositionPredictContainer } from "./container-class";
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
import { prewarmCategory, runDayBasePrewarm } from "./day-base-prewarm";
import {
  enqueueDeliveryCanary,
  listDeliveryCanaries,
  shouldRunDeliveryCanaryCron,
} from "./delivery-canary";
import { DLQ_QUEUE_NAME, handleDlqQueue } from "./dlq-consumer";
import { CONTAINER_SLOT_STALE_MS, type ContainerSlotKind } from "./container-slot-cap";
import {
  claimContainerSlot,
  claimRescoreRace,
  clearContainerSlot,
  completeFocusedFullRace,
  releaseContainerSlot,
} from "./do-state";
import { warmNeon } from "./neon-warm";
import { PREDICT_DO_NAME_PREFIX, resolvePredictDoName } from "./predict-do-shard";
import { PredictRunCoordinator } from "./predict-run-coordinator";
import { hasRequiredPerRaceScope, PER_RACE_SCOPE_REQUIRED_ERROR } from "./per-race-scope-guard";
import { retryPopulateViewerDisplayCache } from "./prediction-cache-warm";
import { getPredictionReadiness } from "./prediction-readiness";
import { handleQueue } from "./queue-consumer";
import { enqueuePredict } from "./queue-producer";
import {
  DEFAULT_RESCORE_LEAD_MINUTES,
  resolveCardMaxRaceBangoForKochi,
  runRaceCoordinatorTick,
} from "./race-coordinator";
import {
  runRunningStyleKickMorningGap,
  runRunningStyleKickTomorrowPrewarm,
  shouldRunRunningStyleKickMorningGapCron,
  shouldRunRunningStyleKickTomorrowPrewarmCron,
} from "./running-style-kick";
import { getRunDateJst, getRunYmdJst } from "./time";
import { isAuthorized, isTriggerRequest, parseRunDates } from "./trigger";
import type {
  DeliveryCanaryMessage,
  Env,
  PredictCategory,
  PredictMode,
  PredictQueueMessage,
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
const FOCUSED_FULL_SLOT_KIND: ContainerSlotKind = "focused-full";
const ADMIN_STOP_CONTAINERS_PATH = "/api/admin/stop-predict-containers";
const ADMIN_COMPLETE_FOCUSED_FULL_RACE_PATH = "/api/admin/complete-focused-full-race";
const ADMIN_RUN_FOCUSED_FULL_RACE_PATH = "/api/admin/run-focused-full-race";
const ADMIN_PREWARM_DAY_BASE_PATH = "/api/admin/prewarm-day-base";
const ADMIN_STOP_CONTAINER_DO_PATH = "/__admin/stop-container";
const PREDICT_DO_HOST = "http://do";
const PREDICT_PATH = "/predict";
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
const RUN_DATE_SEPARATOR = "-";

interface InternalRescoreRaceRequest {
  category: PredictCategory;
  debug?: boolean;
  // See PredictUrlParams.force in queue-consumer.ts (Defect H) -- forwarded
  // to the container so its row-count-only completion check can be bypassed
  // for an admin-triggered focused-full re-run too.
  force?: boolean;
  keibajoCode: string;
  raceBango: string;
  runYmd: string;
}

interface AdminCompleteFocusedFullRaceRequest extends InternalRescoreRaceRequest {
  status: "error" | "success";
}

interface AdminPrewarmDayBaseRequest {
  category?: PredictCategory;
  runYmd: string;
}

interface StopContainerResult {
  name: string;
  ok: boolean;
  status: number;
}

interface ReleaseContainerSlotBestEffortParams {
  doName: string;
  env: Env;
  kind: ContainerSlotKind;
}

interface AttachSlotReleaseOnStreamEndParams extends ReleaseContainerSlotBestEffortParams {
  response: Response;
}

export { FinishPositionPredictContainer, PredictRunCoordinator };

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

export const isAdminPrewarmDayBaseRequest = (method: string, pathname: string): boolean =>
  method === INTERNAL_RESCORE_RACE_METHOD && pathname === ADMIN_PREWARM_DAY_BASE_PATH;

const isValidRescoreCategory = (value: unknown): value is PredictCategory =>
  typeof value === "string" && VALID_CATEGORIES.has(value);

const isValidNonEmptyString = (value: unknown): value is string =>
  typeof value === "string" && value.trim().length > 0;

const isValidRunYmd = (value: unknown): value is string =>
  typeof value === "string" && value.length === RUN_YMD_LENGTH && RUN_YMD_PATTERN.test(value);

const isFocusedFullTerminalStatus = (
  value: unknown,
): value is AdminCompleteFocusedFullRaceRequest["status"] =>
  value === "error" || value === "success";

const parseInternalRescoreRaceBody = (
  body: Record<string, unknown>,
): InternalRescoreRaceRequest | null => {
  const category = body[CATEGORY_FIELD];
  const keibajoCode = body[KEIBAJO_CODE_FIELD];
  const raceBango = body[RACE_BANGO_FIELD];
  const runYmd = body[RUN_YMD_FIELD];
  if (!isValidRescoreCategory(category)) return null;
  if (!isValidNonEmptyString(keibajoCode)) return null;
  if (!isValidNonEmptyString(raceBango)) return null;
  if (!isValidRunYmd(runYmd)) return null;
  return {
    category,
    ...(resolveDebugFlag(body) ? { debug: true } : {}),
    ...(body[FORCE_FIELD] === true ? { force: true } : {}),
    keibajoCode: keibajoCode.trim(),
    raceBango: raceBango.trim(),
    runYmd,
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
  if (category === undefined) return { runYmd };
  if (!isValidRescoreCategory(category)) return null;
  return { category, runYmd };
};

const parseAdminCompleteFocusedFullRaceBody = (
  body: Record<string, unknown>,
): AdminCompleteFocusedFullRaceRequest | null => {
  const parsed = parseInternalRescoreRaceBody(body);
  if (!parsed || !isFocusedFullTerminalStatus(body.status)) return null;
  return { ...parsed, status: body.status };
};

const buildRunDateFromYmd = (runYmd: string): string =>
  [
    runYmd.slice(0, RUN_YMD_YEAR_END),
    runYmd.slice(RUN_YMD_YEAR_END, RUN_YMD_MONTH_END),
    runYmd.slice(RUN_YMD_MONTH_END, RUN_YMD_LENGTH),
  ].join(RUN_DATE_SEPARATOR);

const describeRaceRequest = (body: InternalRescoreRaceRequest): string =>
  `category=${body.category} runYmd=${body.runYmd} keibajo=${body.keibajoCode} race=${body.raceBango}`;

const sendRescoreRaceMessage = async (
  env: Env,
  body: InternalRescoreRaceRequest,
): Promise<void> => {
  const runDate = buildRunDateFromYmd(body.runYmd);
  await env.PREDICT_QUEUE.send({
    category: body.category,
    daysAhead: RESCORE_DAYS_AHEAD,
    ...(body.debug ? { debug: true } : {}),
    keibajoCode: body.keibajoCode,
    mode: RESCORE_MODE,
    raceBango: body.raceBango,
    runDate,
    runDateIso: runDate,
    runYmd: body.runYmd,
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
  const claim = await claimRescoreRace({
    category: parsed.category,
    env,
    keibajoCode: parsed.keibajoCode,
    raceBango: parsed.raceBango,
    runYmd: parsed.runYmd,
  });
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
  await sendRescoreRaceMessage(env, parsed);
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

const parseStopContainerNames = (body: Record<string, unknown>): string[] | null => {
  const names = body.names;
  if (!Array.isArray(names)) return null;
  if (names.length === 0 || names.length > MAX_ADMIN_STOP_NAMES) return null;
  const parsed = names.filter(
    (name): name is string => typeof name === "string" && name.startsWith(PREDICT_DO_NAME_PREFIX),
  );
  return parsed.length === names.length ? parsed : null;
};

const stopPredictContainer = async (
  env: Env,
  authorization: string,
  name: string,
): Promise<StopContainerResult> => {
  const startedAt = Date.now();
  console.warn(`[predict-worker] admin-stop container start name=${name}`);
  const doId = env.FINISH_POSITION_PREDICT_CONTAINER.idFromName(name);
  const stub = env.FINISH_POSITION_PREDICT_CONTAINER.get(doId);
  const response = await stub.fetch(
    new Request(`http://do${ADMIN_STOP_CONTAINER_DO_PATH}`, {
      headers: { authorization },
      method: INTERNAL_RESCORE_RACE_METHOD,
    }),
  );
  console.warn(
    `[predict-worker] admin-stop container response name=${name} status=${
      response.status
    } ok=${response.ok} durationMs=${Date.now() - startedAt}`,
  );
  if (response.ok) {
    try {
      await clearContainerSlot({ doName: name, env });
    } catch (err) {
      console.error(
        `[predict-worker] admin-stop failed to clear container slot name=${name}:`,
        String(err),
      );
    }
  }
  return { name, ok: response.ok, status: response.status };
};

const releaseContainerSlotBestEffort = async (
  params: ReleaseContainerSlotBestEffortParams,
): Promise<void> => {
  try {
    await releaseContainerSlot({
      doName: params.doName,
      env: params.env,
      kind: params.kind,
    });
  } catch (err) {
    console.error(
      `[predict-worker] failed to release container slot doName=${params.doName} kind=${params.kind}:`,
      String(err),
    );
  }
};

const attachSlotReleaseOnStreamEnd = async (
  params: AttachSlotReleaseOnStreamEndParams,
): Promise<Response> => {
  if (params.response.body === null) {
    await releaseContainerSlotBestEffort(params);
    return params.response;
  }
  const released = { done: false };
  const releaseOnce = async (): Promise<void> => {
    if (released.done) return;
    released.done = true;
    await releaseContainerSlotBestEffort(params);
  };
  return new Response(
    params.response.body.pipeThrough(
      new TransformStream<Uint8Array, Uint8Array>({
        transform(chunk, controller) {
          controller.enqueue(chunk);
        },
        flush: releaseOnce,
      }),
    ),
    {
      headers: params.response.headers,
      status: params.response.status,
      statusText: params.response.statusText,
    },
  );
};

const buildFocusedFullPredictUrl = (
  body: InternalRescoreRaceRequest,
  cardMaxRaceBango: number | undefined,
): string => {
  const searchParams = new URLSearchParams({
    category: body.category,
    daysAhead: String(RESCORE_DAYS_AHEAD),
    keibajoCode: body.keibajoCode,
    mode: DEFAULT_MODE,
    raceBango: body.raceBango,
    runDate: body.runYmd,
  });
  if (body.debug === true) searchParams.set("debug", "1");
  if (body.force === true) searchParams.set("force", "1");
  if (cardMaxRaceBango !== undefined) {
    searchParams.set("cardMaxRaceBango", String(cardMaxRaceBango));
  }
  return `${PREDICT_DO_HOST}${PREDICT_PATH}?${searchParams.toString()}`;
};

const runFocusedFullRace = async (
  env: Env,
  body: InternalRescoreRaceRequest,
): Promise<Response> => {
  const startedAt = Date.now();
  const cardMaxRaceBango = await resolveCardMaxRaceBangoForKochi({
    env,
    keibajoCode: body.keibajoCode,
    runYmd: body.runYmd,
  });
  const predictUrl = buildFocusedFullPredictUrl(body, cardMaxRaceBango);
  const doName = resolvePredictDoName({
    category: body.category,
    env,
    keibajoCode: body.keibajoCode,
    raceBango: body.raceBango,
  });
  const claim = await claimContainerSlot({
    category: body.category,
    doName,
    env,
    kind: FOCUSED_FULL_SLOT_KIND,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
  });
  if (!claim.proceed) {
    console.warn(
      `[predict-worker] container slot ${claim.state ?? "capped"} doName=${doName} kind=${FOCUSED_FULL_SLOT_KIND} ${describeRaceRequest(body)}`,
    );
    return Response.json(
      { error: "container slot unavailable", ok: false, state: claim.state ?? "capped" },
      { status: HTTP_SERVICE_UNAVAILABLE },
    );
  }
  if (body.debug) {
    console.warn(`[predict-worker] admin-run-focused-full start ${describeRaceRequest(body)}`);
  }
  const doId = env.FINISH_POSITION_PREDICT_CONTAINER.idFromName(doName);
  const stub = env.FINISH_POSITION_PREDICT_CONTAINER.get(doId);
  try {
    const response = await stub.fetch(new Request(predictUrl));
    if (body.debug) {
      console.warn(
        `[predict-worker] admin-run-focused-full response ${describeRaceRequest(body)} status=${
          response.status
        } ok=${response.ok} durationMs=${Date.now() - startedAt}`,
      );
    }
    return attachSlotReleaseOnStreamEnd({
      doName,
      env,
      kind: FOCUSED_FULL_SLOT_KIND,
      response,
    });
  } catch (err) {
    await releaseContainerSlotBestEffort({
      doName,
      env,
      kind: FOCUSED_FULL_SLOT_KIND,
    });
    throw err;
  }
};

const handleAdminStopContainers = async (request: Request, env: Env): Promise<Response> => {
  const authorization = request.headers.get("authorization");
  if (authorization === null || !isAuthorized(authorization, env.TRIGGER_TOKEN)) {
    console.warn("[predict-worker] admin-stop unauthorized");
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }
  const body = await parseBody(request);
  const names = parseStopContainerNames(body);
  if (!names) {
    console.warn("[predict-worker] admin-stop invalid names");
    return Response.json({ error: "invalid names", ok: false }, { status: HTTP_BAD_REQUEST });
  }
  console.warn(
    `[predict-worker] admin-stop requested count=${names.length} names=${names.join(",")}`,
  );
  const results: StopContainerResult[] = [];
  for (const name of names) {
    results.push(await stopPredictContainer(env, authorization, name));
  }
  console.warn(
    `[predict-worker] admin-stop completed ok=${results.every((result) => result.ok)} results=${JSON.stringify(
      results,
    )}`,
  );
  return Response.json({ ok: results.every((result) => result.ok), results });
};

const guardedAdminStopContainers = async (request: Request, env: Env): Promise<Response> => {
  try {
    return await handleAdminStopContainers(request, env);
  } catch (error) {
    return Response.json({ error: String(error), ok: false }, { status: HTTP_BAD_REQUEST });
  }
};

const handleAdminRunFocusedFullRace = async (
  request: Request,
  env: Env,
  ctx?: ExecutionContext,
): Promise<Response> => {
  if (!isAuthorized(request.headers.get("authorization"), env.TRIGGER_TOKEN)) {
    console.warn("[predict-worker] admin-run-focused-full unauthorized");
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }
  const raw = await parseBody(request);
  const parsed = parseInternalRescoreRaceBody(raw);
  if (!parsed) {
    console.warn("[predict-worker] admin-run-focused-full invalid request");
    return Response.json({ error: "invalid request", ok: false }, { status: HTTP_BAD_REQUEST });
  }
  const response = await runFocusedFullRace(env, parsed);
  const populate = retryPopulateViewerDisplayCache({
    category: parsed.category,
    env,
    keibajoCode: parsed.keibajoCode,
    raceBango: parsed.raceBango,
    runYmd: parsed.runYmd,
  });
  if (ctx === undefined) {
    void populate;
  } else {
    ctx.waitUntil(populate);
  }
  return new Response(response.body, {
    headers: response.headers,
    status: response.status,
  });
};

const guardedAdminRunFocusedFullRace = async (
  request: Request,
  env: Env,
  ctx?: ExecutionContext,
): Promise<Response> => {
  try {
    return await handleAdminRunFocusedFullRace(request, env, ctx);
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
  const daysAhead = Number(env.PREDICT_DAYS_AHEAD);
  console.warn(
    `[predict-worker] admin-prewarm-day-base start runYmd=${parsed.runYmd} category=${parsed.category ?? "all"}`,
  );
  const landed =
    parsed.category === undefined
      ? await runDayBasePrewarm({ daysAhead, env, runYmd: parsed.runYmd })
      : await prewarmCategory({
          category: parsed.category,
          daysAhead,
          env,
          runYmd: parsed.runYmd,
        });
  return Response.json({
    category: parsed.category ?? "all",
    ok: landed,
    runYmd: parsed.runYmd,
  });
};

const guardedAdminPrewarmDayBase = async (request: Request, env: Env): Promise<Response> => {
  try {
    return await handleAdminPrewarmDayBase(request, env);
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
  console.warn(
    `[predict-worker] admin-complete-focused-full start ${describeRaceRequest(parsed)} status=${parsed.status}`,
  );
  await completeFocusedFullRace({ env, ...parsed });
  console.warn(
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
  const readiness = await getPredictionReadiness({ env, now, runYmd: getRunYmdJst(now) });
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

export const handleFetch = async (
  request: Request,
  env: Env,
  ctx?: ExecutionContext,
): Promise<Response> => {
  const url = new URL(request.url);
  if (isTriggerRequest(request.method, url.pathname)) {
    return guardedTrigger(request, env);
  }
  if (isAdminStopContainersRequest(request.method, url.pathname)) {
    return guardedAdminStopContainers(request, env);
  }
  if (isAdminCompleteFocusedFullRaceRequest(request.method, url.pathname)) {
    return guardedAdminCompleteFocusedFullRace(request, env);
  }
  if (isAdminRunFocusedFullRaceRequest(request.method, url.pathname)) {
    return guardedAdminRunFocusedFullRace(request, env, ctx);
  }
  if (isAdminPrewarmDayBaseRequest(request.method, url.pathname)) {
    return guardedAdminPrewarmDayBase(request, env);
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
    // Per-race timing layer: enqueue rescore messages for races within T-X of
    // post time, scoped to env.RESCORE_CATEGORIES (JRA only as of 2026-07-11).
    // Routes to the existing container held /predict mode=rescore path — does
    // not start a new container class or touch the predict / warm crons. A
    // shadow no-op when env.COORDINATOR_ENABLED !== "1".
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
  batch: MessageBatch<PredictQueueMessage | DeliveryCanaryMessage>,
  env: Env,
): Promise<void> => {
  if (batch.queue === DLQ_QUEUE_NAME) {
    await handleDlqQueue(batch, env);
    return;
  }
  await handleQueue(batch, env);
};

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    return handleFetch(request, env, ctx);
  },
  async scheduled(event: ScheduledEvent, env: Env): Promise<void> {
    await handleScheduled(event, env);
  },
  async queue(
    batch: MessageBatch<PredictQueueMessage | DeliveryCanaryMessage>,
    env: Env,
  ): Promise<void> {
    await handleQueueBatch(batch, env);
  },
};
