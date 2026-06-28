// Run with bun. Fetch (health + on-demand trigger) + scheduled (cron -> container) + queue handlers.

import { getContainer } from "@cloudflare/containers";
import { buildAuditBindParams, buildAuditInsertSql, buildAuditRecord } from "./audit";
import { FinishPositionPredictContainer } from "./container-class";
import {
  enumerateTodaysRaces,
  PREDICT_CRON,
  shouldRunCoordinatorCron,
  shouldRunFeatureBuildCron,
  shouldRunPredictCron,
  shouldRunRescoreCron,
  shouldRunWarmCron,
} from "./cron-decision";
import { buildPredictStartOptions } from "./dispatch";
import { claimRescoreRace } from "./do-state";
import { warmNeon } from "./neon-warm";
import { PredictRunCoordinator } from "./predict-run-coordinator";
import { handleQueue } from "./queue-consumer";
import { enqueuePredict } from "./queue-producer";
import { DEFAULT_RESCORE_LEAD_MINUTES, runRaceCoordinatorTick } from "./race-coordinator";
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
const RUN_YMD_FIELD = "runYmd";
const DEFAULT_MODE: PredictMode = "full";
const FULL_MODE: PredictMode = "full";
const RESCORE_MODE: PredictMode = "rescore";
const VALID_MODES: ReadonlySet<string> = new Set(["full", "rescore"]);
const VALID_CATEGORIES: ReadonlySet<string> = new Set(["jra", "nar", "ban-ei"]);
const RESCORE_DAYS_AHEAD = 0;
const HTTP_OK = 200;
const HTTP_UNAUTHORIZED = 401;
const HTTP_BAD_REQUEST = 400;
const HTTP_ACCEPTED = 202;
const INTERNAL_RESCORE_RACE_PATH = "/api/internal/rescore-race";
const INTERNAL_RESCORE_RACE_METHOD = "POST";
const RUN_YMD_LENGTH = 8;
const RUN_YMD_YEAR_END = 4;
const RUN_YMD_MONTH_END = 6;
const RUN_YMD_PATTERN = /^\d{8}$/u;
const RUN_DATE_SEPARATOR = "-";

interface InternalRescoreRaceRequest {
  category: PredictCategory;
  keibajoCode: string;
  raceBango: string;
  runYmd: string;
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
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }
  const body = await parseBody(request);
  const dates = resolveTriggerDates(body);
  const mode = resolveMode(body);
  const skipDedup = body[SKIP_DEDUP_FIELD] === true;
  const queued = await enqueuePredict({
    category: resolveCategory(body),
    daysAhead: Number(env.PREDICT_DAYS_AHEAD),
    env,
    keibajoCode: resolveRaceTargetField(body, KEIBAJO_CODE_FIELD),
    mode,
    raceBango: resolveRaceTargetField(body, RACE_BANGO_FIELD),
    runDate: dates.runDate,
    runYmd: dates.runYmd,
    ...(skipDedup ? { skipDedup: true } : {}),
  });
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

const isValidRescoreCategory = (value: unknown): value is PredictCategory =>
  typeof value === "string" && VALID_CATEGORIES.has(value);

const isValidNonEmptyString = (value: unknown): value is string =>
  typeof value === "string" && value.trim().length > 0;

const isValidRunYmd = (value: unknown): value is string =>
  typeof value === "string" && value.length === RUN_YMD_LENGTH && RUN_YMD_PATTERN.test(value);

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
    keibajoCode: keibajoCode.trim(),
    raceBango: raceBango.trim(),
    runYmd,
  };
};

const buildRunDateFromYmd = (runYmd: string): string =>
  [
    runYmd.slice(0, RUN_YMD_YEAR_END),
    runYmd.slice(RUN_YMD_YEAR_END, RUN_YMD_MONTH_END),
    runYmd.slice(RUN_YMD_MONTH_END, RUN_YMD_LENGTH),
  ].join(RUN_DATE_SEPARATOR);

const sendRescoreRaceMessage = async (
  env: Env,
  body: InternalRescoreRaceRequest,
): Promise<void> => {
  const runDate = buildRunDateFromYmd(body.runYmd);
  await env.PREDICT_QUEUE.send({
    category: body.category,
    daysAhead: RESCORE_DAYS_AHEAD,
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
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }
  const raw = await parseBody(request);
  const parsed = parseInternalRescoreRaceBody(raw);
  if (!parsed) {
    return Response.json({ error: "invalid request", ok: false }, { status: HTTP_BAD_REQUEST });
  }
  const claim = await claimRescoreRace({
    category: parsed.category,
    env,
    keibajoCode: parsed.keibajoCode,
    raceBango: parsed.raceBango,
    runYmd: parsed.runYmd,
  });
  if (!claim.proceed) {
    return Response.json({ claimed: false, ok: true }, { status: HTTP_OK });
  }
  await sendRescoreRaceMessage(env, parsed);
  return Response.json({ claimed: true, ok: true }, { status: HTTP_ACCEPTED });
};

const guardedInternalRescoreRace = async (request: Request, env: Env): Promise<Response> => {
  try {
    return await handleInternalRescoreRace(request, env);
  } catch (error) {
    return Response.json({ error: String(error), ok: false }, { status: HTTP_BAD_REQUEST });
  }
};

export const handleFetch = async (request: Request, env: Env): Promise<Response> => {
  const url = new URL(request.url);
  if (isTriggerRequest(request.method, url.pathname)) {
    return guardedTrigger(request, env);
  }
  if (isInternalRescoreRaceRequest(request.method, url.pathname)) {
    return guardedInternalRescoreRace(request, env);
  }
  return healthResponse();
};

// Fan out one full-mode per-race build per race in today's realtime_race_sources
// so the Container builds features + predicts a single race at a time instead of
// one 21y full-batch scan. Enqueues nothing when no races run today.
const enqueuePerRaceFeatureBuilds = async (env: Env, scheduledAt: Date): Promise<void> => {
  const runDate = getRunDateJst(scheduledAt);
  const runYmd = getRunYmdJst(scheduledAt);
  const races = await enumerateTodaysRaces(env.REALTIME_DB, runYmd);
  const daysAhead = Number(env.PREDICT_DAYS_AHEAD);
  await Promise.all(
    races.map((race) =>
      enqueuePredict({
        category: race.category,
        daysAhead,
        env,
        keibajoCode: race.keibajoCode,
        mode: FULL_MODE,
        raceBango: race.raceBango,
        runDate,
        runYmd,
      }),
    ),
  );
};

export const handleScheduled = async (event: ScheduledEvent, env: Env): Promise<void> => {
  if (shouldRunWarmCron(event.cron)) {
    await warmNeon(env.NEON_DATABASE_URL);
    return;
  }
  if (shouldRunCoordinatorCron(event.cron)) {
    // Per-race timing layer: enqueue rescore messages for races within T-X of
    // post time. Shadow-safe — the rescore consumer (task B) is not wired yet,
    // so an enqueued message is a no-op for production predictions. Does not
    // start the container or touch the predict / warm crons.
    await runRaceCoordinatorTick({
      env,
      leadMinutes: DEFAULT_RESCORE_LEAD_MINUTES,
      now: new Date(event.scheduledTime),
    });
    return;
  }
  if (shouldRunFeatureBuildCron(event.cron)) {
    // Enqueue one full-mode build per race in today's realtime_race_sources so
    // the Container builds + scores a single race at a time (no 21y full-batch
    // scan). COORDINATOR_ENABLED does not gate this path (it only gates the
    // per-race rescore coordinator).
    await enqueuePerRaceFeatureBuilds(env, new Date(event.scheduledTime));
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

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    return handleFetch(request, env);
  },
  async scheduled(event: ScheduledEvent, env: Env): Promise<void> {
    await handleScheduled(event, env);
  },
  async queue(batch: MessageBatch<PredictQueueMessage>, env: Env): Promise<void> {
    await handleQueue(batch, env);
  },
};
