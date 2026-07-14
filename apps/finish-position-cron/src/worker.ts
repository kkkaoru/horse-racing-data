// Run with bun. Fetch (health + on-demand trigger) + scheduled (cron -> container) + queue handlers.

import { getContainer } from "@cloudflare/containers";
import { buildAuditBindParams, buildAuditInsertSql, buildAuditRecord } from "./audit";
import { FinishPositionPredictContainer } from "./container-class";
import { runCoverageSelfHeal, shouldRunCoverageSelfHealCron } from "./coverage-self-heal";
import {
  PREDICT_CRON,
  shouldRunCoordinatorCron,
  shouldRunFeatureBuildCron,
  shouldRunPredictCron,
  shouldRunRescoreCron,
  shouldRunWarmCron,
} from "./cron-decision";
import { runDayBasePrewarm } from "./day-base-prewarm";
import { buildPredictStartOptions } from "./dispatch";
import { DLQ_QUEUE_NAME, handleDlqQueue } from "./dlq-consumer";
import { claimRescoreRace, completeFocusedFullRace } from "./do-state";
import { warmNeon } from "./neon-warm";
import { PredictRunCoordinator } from "./predict-run-coordinator";
import { handleQueue } from "./queue-consumer";
import { enqueuePredict } from "./queue-producer";
import {
  DEFAULT_RESCORE_LEAD_MINUTES,
  resolveCardMaxRaceBangoForKochi,
  runRaceCoordinatorTick,
} from "./race-coordinator";
import { getRunDateJst, getRunYmdJst } from "./time";
import { isAuthorized, isTriggerRequest, parseRunDates } from "./trigger";
import type {
  CronAuditRecord,
  Env,
  PredictCategory,
  PredictMode,
  PredictQueueMessage,
  RunDates,
} from "./types";

const CONTAINER_INSTANCE_NAME = "daily-finish-position-predict";
const ZERO_RACES = 0;
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
const ADMIN_STOP_CONTAINERS_PATH = "/api/admin/stop-predict-containers";
const ADMIN_COMPLETE_FOCUSED_FULL_RACE_PATH = "/api/admin/complete-focused-full-race";
const ADMIN_RUN_FOCUSED_FULL_RACE_PATH = "/api/admin/run-focused-full-race";
const ADMIN_STOP_CONTAINER_DO_PATH = "/__admin/stop-container";
const PREDICT_DO_HOST = "http://do";
const PREDICT_PATH = "/predict";
const PREDICT_CONTAINER_NAME_PREFIX = "predict-";
const MAX_ADMIN_STOP_NAMES = 100;
const INTERNAL_RESCORE_RACE_PATH = "/api/internal/rescore-race";
const INTERNAL_RESCORE_RACE_METHOD = "POST";
const RUN_YMD_LENGTH = 8;
const RUN_YMD_YEAR_END = 4;
const RUN_YMD_MONTH_END = 6;
const RUN_YMD_PATTERN = /^\d{8}$/u;
const RUN_DATE_SEPARATOR = "-";

interface InternalRescoreRaceRequest {
  category: PredictCategory;
  debug?: boolean;
  keibajoCode: string;
  raceBango: string;
  runYmd: string;
}

interface AdminCompleteFocusedFullRaceRequest extends InternalRescoreRaceRequest {
  status: "error" | "success";
}

interface StopContainerResult {
  name: string;
  ok: boolean;
  status: number;
}

export { FinishPositionPredictContainer, PredictRunCoordinator };

const healthResponse = (): Response =>
  Response.json({ cron: PREDICT_CRON, name: "finish-position-cron", ok: true });

const recordAudit = async (env: Env, record: CronAuditRecord): Promise<void> => {
  await env.FINISH_POSITION_CRON_DB.prepare(buildAuditInsertSql())
    .bind(...buildAuditBindParams(record))
    .run();
};

const runPrediction = async (env: Env, dates: RunDates): Promise<void> => {
  const startedAt = Date.now();
  const container = getContainer(env.FINISH_POSITION_PREDICT_CONTAINER, CONTAINER_INSTANCE_NAME);
  await container.start(
    buildPredictStartOptions({ env, runDate: dates.runDate, runYmd: dates.runYmd }),
  );
  await recordAudit(
    env,
    buildAuditRecord({
      durationMs: Date.now() - startedAt,
      error: null,
      racesPredicted: ZERO_RACES,
      runDate: dates.runDate,
      status: "started",
    }),
  );
};

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
  if (debug) {
    console.log(
      `[predict-worker] trigger enqueue start runDate=${dates.runDate} runYmd=${dates.runYmd} category=${
        category ?? "-"
      } mode=${mode} keibajo=${keibajoCode ?? "-"} race=${raceBango ?? "-"} skipDedup=${skipDedup} debug=true`,
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

export const isAdminStopContainersRequest = (method: string, pathname: string): boolean =>
  method === INTERNAL_RESCORE_RACE_METHOD && pathname === ADMIN_STOP_CONTAINERS_PATH;

export const isAdminCompleteFocusedFullRaceRequest = (method: string, pathname: string): boolean =>
  method === INTERNAL_RESCORE_RACE_METHOD && pathname === ADMIN_COMPLETE_FOCUSED_FULL_RACE_PATH;

export const isAdminRunFocusedFullRaceRequest = (method: string, pathname: string): boolean =>
  method === INTERNAL_RESCORE_RACE_METHOD && pathname === ADMIN_RUN_FOCUSED_FULL_RACE_PATH;

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
    keibajoCode: keibajoCode.trim(),
    raceBango: raceBango.trim(),
    runYmd,
  };
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
    (name): name is string =>
      typeof name === "string" && name.startsWith(PREDICT_CONTAINER_NAME_PREFIX),
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
  return { name, ok: response.ok, status: response.status };
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
  if (body.debug) {
    console.warn(`[predict-worker] admin-run-focused-full start ${describeRaceRequest(body)}`);
  }
  const doId = env.FINISH_POSITION_PREDICT_CONTAINER.idFromName(
    `${PREDICT_CONTAINER_NAME_PREFIX}${body.category}`,
  );
  const stub = env.FINISH_POSITION_PREDICT_CONTAINER.get(doId);
  const response = await stub.fetch(new Request(predictUrl));
  if (body.debug) {
    console.warn(
      `[predict-worker] admin-run-focused-full response ${describeRaceRequest(body)} status=${
        response.status
      } ok=${response.ok} durationMs=${Date.now() - startedAt}`,
    );
  }
  return response;
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

const handleAdminRunFocusedFullRace = async (request: Request, env: Env): Promise<Response> => {
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
  return new Response(response.body, {
    headers: response.headers,
    status: response.status,
  });
};

const guardedAdminRunFocusedFullRace = async (request: Request, env: Env): Promise<Response> => {
  try {
    return await handleAdminRunFocusedFullRace(request, env);
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

export const handleFetch = async (request: Request, env: Env): Promise<Response> => {
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
    return guardedAdminRunFocusedFullRace(request, env);
  }
  if (isInternalRescoreRaceRequest(request.method, url.pathname)) {
    return guardedInternalRescoreRace(request, env);
  }
  return healthResponse();
};

export const handleScheduled = async (event: ScheduledEvent, env: Env): Promise<void> => {
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
    // Per-race coverage self-healing scan (doc §4.3,
    // docs/cf-only-serving-architecture.md): the direct functional
    // replacement for race-prediction-guard.sh's day-wide COUNT check.
    // Re-enqueues only the specific races whose post time is >15 min past
    // and still missing a complete prediction, respecting the existing
    // focused-full DO claim/heartbeat/staleness semantics -- never a
    // day-wide re-run. See coverage-self-heal.ts.
    await runCoverageSelfHeal({ env, now: new Date(event.scheduledTime) });
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
    // Enqueue rescore messages for all categories (race-hours freshness).
    // daysAhead=0: only today's races need re-scoring.
    const scheduledAt = new Date(event.scheduledTime);
    await enqueuePredict({
      daysAhead: RESCORE_DAYS_AHEAD,
      env,
      mode: "rescore",
      runDate: getRunDateJst(scheduledAt),
      runYmd: getRunYmdJst(scheduledAt),
    });
    return;
  }
  if (!shouldRunPredictCron(event.cron)) {
    return;
  }
  const scheduledAt = new Date(event.scheduledTime);
  await runPrediction(env, {
    runDate: getRunDateJst(scheduledAt),
    runYmd: getRunYmdJst(scheduledAt),
  });
};

// A single Worker script consumes both the primary predict queue and its
// dead-letter queue (two consumer entries in wrangler.jsonc, both routed to
// this one `queue()` export); MessageBatch.queue names which queue the batch
// came from, so route accordingly instead of running the two independent
// consumer implementations against the wrong batch shape.
export const handleQueueBatch = async (
  batch: MessageBatch<PredictQueueMessage>,
  env: Env,
): Promise<void> => {
  if (batch.queue === DLQ_QUEUE_NAME) {
    await handleDlqQueue(batch, env);
    return;
  }
  await handleQueue(batch, env);
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    return handleFetch(request, env);
  },
  async scheduled(event: ScheduledEvent, env: Env): Promise<void> {
    await handleScheduled(event, env);
  },
  async queue(batch: MessageBatch<PredictQueueMessage>, env: Env): Promise<void> {
    await handleQueueBatch(batch, env);
  },
};
