import {
  getCachedProviderCursor,
  purgeAllProviderCursorCaches,
  purgeProviderCursorCache,
} from "./cache";
import { isPermanentJobError, PermanentJobError } from "./errors";
import { acquireRun, handleJob, recordJobFailure } from "./jobs";
import { layoutByTable } from "./layouts";
import {
  BOOTSTRAP_LOOKBACK_DAYS,
  DEFAULT_LOOKBACK_DAYS,
  defaultAcquisitionWindow,
  jstDate,
  jstTimestamp,
  scheduledAction,
} from "./schedule";
import {
  createRun,
  getDailyRun,
  getLatestRun,
  getProviderAcquisitionCursor,
  getReadyIndexPartitions,
  getRun,
  listCatalogTargets,
  markParsed,
  updateRunStatus,
} from "./state";
import type { AcquisitionWindow } from "./schedule";
import { createTableStage, tableStagingKey } from "./source-stream";
import type { Env, Provider, RecordRow, RecordValue, RunRow, SyncJob, TriggerKind } from "./types";

const MAX_LOOKBACK_DAYS = 31;
const JV_RAW_STAGE_QUEUE = "daily-keiba-sync-jv-raw-stage-jobs";
const NEON_QUEUE = "daily-keiba-sync-neon-jobs";
const NV_RAW_STAGE_QUEUE = "daily-keiba-sync-nv-raw-stage-jobs";
const R2_CATALOG_QUEUE = "daily-keiba-sync-r2-catalog-jobs";
const STALE_RUN_MS = 20 * 60 * 1000;
const EXTERNAL_TABLE_NAME = "netkeiba_training_workouts";
const MAX_EXTERNAL_STAGE_RECORDS = 2_000;

const isAuthorized = (request: Request, token: string): boolean => {
  const header = request.headers.get("Authorization");
  if (header === null || !header.startsWith("Bearer ")) return false;
  const supplied = header.slice(7);
  if (supplied.length !== token.length) return false;
  let difference = 0;
  for (let index = 0; index < token.length; index += 1)
    difference |= supplied.charCodeAt(index) ^ token.charCodeAt(index);
  return difference === 0;
};

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const parseProvider = (value: unknown): Provider => {
  if (value !== "jv" && value !== "nv") throw new Error("Invalid provider");
  return value;
};

const parseRunDate = (value: unknown, fallback: string): string => {
  const runDate = value === undefined ? fallback : value;
  if (typeof runDate !== "string" || !/^20\d{6}$/.test(runDate))
    throw new Error("Invalid run date");
  return runDate;
};

const parseProviderTime = (value: unknown, field: string): string => {
  if (typeof value !== "string" || !/^20\d{12}$/.test(value)) throw new Error(`Invalid ${field}`);
  const iso = `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}T${value.slice(8, 10)}:${value.slice(10, 12)}:${value.slice(12, 14)}Z`;
  const parsed = new Date(iso);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString() !== `${iso.slice(0, -1)}.000Z`)
    throw new Error(`Invalid ${field}`);
  return value;
};

const parseLookback = (value: unknown, fallback: number): number => {
  const lookback = value === undefined ? fallback : value;
  if (
    !Number.isInteger(lookback) ||
    typeof lookback !== "number" ||
    lookback < 1 ||
    lookback > MAX_LOOKBACK_DAYS
  )
    throw new Error("Invalid lookback days");
  return lookback;
};

const startAcquisition = async (
  env: Env,
  provider: Provider,
  runDate: string,
  trigger: TriggerKind,
  lookbackDays: number,
  force: boolean,
  now: Date,
  requestedWindow?: AcquisitionWindow,
): Promise<RunRow> => {
  const advanceCursor = trigger !== "manual";
  const cursorTime = jstTimestamp(now.getTime());
  const fallbackWindow = defaultAcquisitionWindow(provider, runDate, lookbackDays);
  const previousCursor = advanceCursor
    ? await getCachedProviderCursor(env.SYNC_CACHE, provider, async () =>
        getProviderAcquisitionCursor(env.DB, provider),
      )
    : null;
  const window = requestedWindow ?? {
    fromTime: previousCursor ?? fallbackWindow.fromTime,
    toTime: provider === "jv" ? cursorTime : null,
  };
  const { created, run } = await createRun(
    env.DB,
    provider,
    runDate,
    trigger,
    lookbackDays,
    now,
    force,
    window.fromTime,
    window.toTime,
    cursorTime,
    advanceCursor,
  );
  if (created) {
    try {
      await acquireRun(
        {
          advanceCursor,
          cursorTime,
          fromTime: window.fromTime,
          provider,
          runDate,
          runId: run.run_id,
          toTime: window.toTime,
        },
        env,
        now,
      );
    } catch (error) {
      await updateRunStatus(env.DB, run.run_id, "acquisition_failed", now, "acquisition");
      throw error;
    }
  }
  return run;
};

const monitor = async (env: Env, provider: Provider, runDate: string, now: Date): Promise<void> => {
  const run = await getDailyRun(env.DB, provider, runDate);
  if (run === null) {
    await startAcquisition(env, provider, runDate, "monitor", BOOTSTRAP_LOOKBACK_DAYS, false, now);
    return;
  }
  if (run.status === "succeeded" || run.status === "succeeded_empty") return;
  const updatedAt = Date.parse(run.updated_at);
  if (Number.isFinite(updatedAt) && now.getTime() - updatedAt < STALE_RUN_MS) return;
  if (run.staging_key === null) {
    const window =
      run.from_time === null
        ? defaultAcquisitionWindow(provider, runDate, BOOTSTRAP_LOOKBACK_DAYS)
        : { fromTime: run.from_time, toTime: run.to_time };
    await acquireRun(
      {
        advanceCursor: run.advance_cursor === 1,
        cursorTime: run.cursor_time ?? jstTimestamp(now.getTime()),
        fromTime: window.fromTime,
        provider,
        runDate,
        runId: run.run_id,
        toTime: window.toTime,
      },
      env,
      now,
    );
    return;
  }
  await env.R2_CATALOG_JOBS.send({ provider, runDate, runId: run.run_id, type: "recover" });
};

const scheduled = async (event: ScheduledController, env: Env): Promise<void> => {
  const action = scheduledAction(event.cron);
  if (action === undefined) throw new Error("Unknown cron trigger");
  const now = new Date(event.scheduledTime);
  const runDate = jstDate(event.scheduledTime);
  if (action.action === "run") {
    await startAcquisition(
      env,
      action.provider,
      runDate,
      "daily",
      DEFAULT_LOOKBACK_DAYS,
      false,
      now,
    );
    return;
  }
  await monitor(env, action.provider, runDate, now);
};

interface AdminRunRequest {
  fromTime: string;
  lookbackDays: number;
  provider: Provider;
  runDate: string;
  toTime: string | null;
}

const parseAdminRunRequest = (value: Record<string, unknown>, now: Date): AdminRunRequest => {
  const provider = parseProvider(value.provider);
  const runDate = parseRunDate(value.runDate, jstDate(now.getTime()));
  const lookbackDays = parseLookback(value.lookbackDays, BOOTSTRAP_LOOKBACK_DAYS);
  const defaultWindow = defaultAcquisitionWindow(provider, runDate, lookbackDays);
  const fromTime =
    value.fromTime === undefined
      ? defaultWindow.fromTime
      : parseProviderTime(value.fromTime, "from time");
  const toTime =
    provider === "nv"
      ? null
      : value.toTime === undefined
        ? defaultWindow.toTime
        : parseProviderTime(value.toTime, "to time");
  if (provider === "nv" && value.toTime !== undefined)
    throw new Error("NV acquisition does not accept an end time");
  if (toTime !== null && fromTime > toTime) throw new Error("Invalid acquisition window");
  return { fromTime, lookbackDays, provider, runDate, toTime };
};

const adminRun = async (request: Request, env: Env, now: Date): Promise<Response> => {
  const value: unknown = await request.json();
  if (!isObject(value)) return Response.json({ error: "Invalid request" }, { status: 400 });
  let parsed: AdminRunRequest;
  try {
    parsed = parseAdminRunRequest(value, now);
  } catch {
    return Response.json({ error: "Invalid request" }, { status: 400 });
  }
  try {
    const run = await startAcquisition(
      env,
      parsed.provider,
      parsed.runDate,
      "manual",
      parsed.lookbackDays,
      true,
      now,
      { fromTime: parsed.fromTime, toTime: parsed.toTime },
    );
    return Response.json({
      fromTime: parsed.fromTime,
      provider: run.provider,
      runDate: run.run_date,
      runId: run.run_id,
      toTime: parsed.toTime,
    });
  } catch {
    return Response.json({ error: "Acquisition failed" }, { status: 502 });
  }
};

const adminTrigger = async (request: Request, env: Env, now: Date): Promise<Response> => {
  const value: unknown = await request.json();
  if (
    !isObject(value) ||
    (value.action !== "run" && value.action !== "monitor") ||
    (value.force !== undefined && typeof value.force !== "boolean")
  )
    return Response.json({ error: "Invalid request" }, { status: 400 });
  let provider: Provider;
  let runDate: string;
  try {
    provider = parseProvider(value.provider);
    runDate = parseRunDate(value.runDate, jstDate(now.getTime()));
    // Future race dates must not reserve a later scheduled acquisition's dedupe key.
    if (runDate > jstDate(now.getTime())) throw new Error("Future daily run date is not allowed");
  } catch {
    return Response.json({ error: "Invalid request" }, { status: 400 });
  }
  try {
    if (value.action === "run") {
      const run = await startAcquisition(
        env,
        provider,
        runDate,
        "daily",
        DEFAULT_LOOKBACK_DAYS,
        value.force ?? false,
        now,
      );
      return Response.json({ action: "run", provider, runDate, runId: run.run_id });
    }
    await monitor(env, provider, runDate, now);
    const run = await getLatestRun(env.DB, provider, runDate);
    return Response.json({ action: "monitor", provider, runDate, runId: run?.run_id ?? null });
  } catch {
    return Response.json({ error: "Trigger failed" }, { status: 502 });
  }
};

const adminIndex = async (request: Request, env: Env, now: Date): Promise<Response> => {
  const value: unknown = await request.json();
  if (!isObject(value)) return Response.json({ error: "Invalid request" }, { status: 400 });
  try {
    const provider = parseProvider(value.provider);
    if (
      typeof value.tableName !== "string" ||
      typeof value.partitionValue !== "string" ||
      !/^20\d{2}$/.test(value.partitionValue)
    )
      throw new Error("Invalid index target");
    const targets = await listCatalogTargets(env.DB, provider);
    if (!targets.some((target) => target.table_name === value.tableName))
      throw new Error("Catalog target is not configured");
    const runId = `index-${crypto.randomUUID()}`;
    const runDate = jstDate(now.getTime());
    await env.R2_CATALOG_JOBS.send({
      partitionValue: value.partitionValue,
      provider,
      runDate,
      runId,
      tableName: value.tableName,
      type: "index-plan",
    });
    return Response.json({ partitionValue: value.partitionValue, tableName: value.tableName });
  } catch {
    return Response.json({ error: "Invalid request" }, { status: 400 });
  }
};

const adminCachePurge = async (request: Request, env: Env): Promise<Response> => {
  const value: unknown = await request.json();
  if (!isObject(value)) return Response.json({ error: "Invalid request" }, { status: 400 });
  try {
    if (value.provider === undefined) {
      await purgeAllProviderCursorCaches(env.SYNC_CACHE);
      return Response.json({ purged: ["jv", "nv"] });
    }
    const provider = parseProvider(value.provider);
    await purgeProviderCursorCache(env.SYNC_CACHE, provider);
    return Response.json({ purged: [provider] });
  } catch {
    return Response.json({ error: "Invalid request" }, { status: 400 });
  }
};

export const parseExternalRecord = (value: unknown): RecordRow => {
  if (!isObject(value)) throw new Error("Invalid external record");
  const layout = layoutByTable(EXTERNAL_TABLE_NAME);
  const names = new Set(Object.keys(value));
  if (names.size !== layout.columns.length) throw new Error("Invalid external record columns");
  const row: Record<string, RecordValue> = {};
  for (const column of layout.columns) {
    if (!names.has(column.name)) throw new Error("Invalid external record columns");
    const field = value[column.name];
    if (field === null) {
      if (layout.primaryKey.includes(column.name) || column.name === "kaisai_nen")
        throw new Error("Invalid external required value");
      row[column.name] = null;
      continue;
    }
    if (column.catalogType === "int") {
      if (typeof field !== "number" || !Number.isInteger(field))
        throw new Error("Invalid external integer value");
      row[column.name] = field;
      continue;
    }
    if (typeof field !== "string") throw new Error("Invalid external string value");
    if (column.catalogType === "timestamptz" && Number.isNaN(Date.parse(field)))
      throw new Error("Invalid external timestamp value");
    row[column.name] = field;
  }
  return row;
};

const stageExternalTable = async (request: Request, env: Env, now: Date): Promise<Response> => {
  const value: unknown = await request.json().catch(() => null);
  if (!isObject(value) || value.tableName !== EXTERNAL_TABLE_NAME || !Array.isArray(value.records))
    return Response.json({ error: "Invalid request" }, { status: 400 });
  let runDate: string;
  let records: readonly RecordRow[];
  try {
    runDate = parseRunDate(value.runDate, jstDate(now.getTime()));
    if (value.records.length < 1 || value.records.length > MAX_EXTERNAL_STAGE_RECORDS)
      throw new Error("Invalid external record count");
    records = value.records.map(parseExternalRecord);
    if (records.some((row) => row.kaisai_nen !== runDate.slice(0, 4)))
      throw new Error("External partition does not match run date");
    const targets = await listCatalogTargets(env.DB, "jv");
    if (!targets.some((target) => target.table_name === EXTERNAL_TABLE_NAME))
      throw new Error("External Catalog target is not configured");
  } catch {
    return Response.json({ error: "Invalid request" }, { status: 400 });
  }
  const { run } = await createRun(env.DB, "jv", runDate, "manual", 1, now, true);
  const stagingKey = tableStagingKey("jv", runDate, run.run_id, EXTERNAL_TABLE_NAME);
  await env.SOURCE_STAGING.put(
    stagingKey,
    JSON.stringify(createTableStage("jv", run.run_id, EXTERNAL_TABLE_NAME, records)),
    { httpMetadata: { contentType: "application/json" } },
  );
  const partitions = [...new Set(records.map((row) => String(row.kaisai_nen)))];
  const ready = await getReadyIndexPartitions(env.DB, EXTERNAL_TABLE_NAME, partitions);
  const catalogStatus = ready.size === partitions.length ? "pending" : "index_pending";
  await markParsed(
    env.DB,
    run.run_id,
    1,
    records.length,
    [
      {
        catalog_status: catalogStatus,
        neon_status: "pending",
        partitions,
        source_records: records.length,
        staging_key: stagingKey,
        table_name: EXTERNAL_TABLE_NAME,
      },
    ],
    now,
  );
  await env.R2_CATALOG_JOBS.send(
    catalogStatus === "pending"
      ? {
          provider: "jv",
          runDate,
          runId: run.run_id,
          tableName: EXTERNAL_TABLE_NAME,
          tableStagingKey: stagingKey,
          type: "catalog-table",
        }
      : {
          partitionValue: partitions[0]!,
          provider: "jv",
          runDate,
          runId: run.run_id,
          tableName: EXTERNAL_TABLE_NAME,
          type: "index-plan",
        },
  );
  return Response.json({ records: records.length, runId: run.run_id }, { status: 202 });
};

const adminStatus = async (request: Request, env: Env): Promise<Response> => {
  const url = new URL(request.url);
  try {
    const provider = parseProvider(url.searchParams.get("provider"));
    const runDate = parseRunDate(url.searchParams.get("runDate") ?? undefined, jstDate(Date.now()));
    const run = await getLatestRun(env.DB, provider, runDate);
    return run === null ? new Response(null, { status: 404 }) : Response.json(run);
  } catch {
    return Response.json({ error: "Invalid query" }, { status: 400 });
  }
};

const fetchHandler = async (request: Request, env: Env): Promise<Response> => {
  const url = new URL(request.url);
  if (request.method === "GET" && url.pathname === "/health")
    return Response.json({ ok: true, runtime: "cloudflare-workers-native-daily-keiba-sync" });
  if (
    (request.method === "POST" && url.pathname === "/internal/stage-netkeiba-training") ||
    (request.method === "GET" && url.pathname === "/internal/run-status")
  ) {
    if (!isAuthorized(request, env.REALTIME_ADMIN_TOKEN))
      return Response.json({ error: "Unauthorized" }, { status: 401 });
    if (request.method === "POST") return await stageExternalTable(request, env, new Date());
    const runId = url.searchParams.get("runId");
    if (runId === null || !/^[0-9a-f-]{36}$/u.test(runId))
      return Response.json({ error: "Invalid query" }, { status: 400 });
    try {
      return Response.json(await getRun(env.DB, runId));
    } catch {
      return new Response(null, { status: 404 });
    }
  }
  if (!isAuthorized(request, env.ADMIN_TOKEN))
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  if (request.method === "POST" && url.pathname === "/admin/run")
    return await adminRun(request, env, new Date());
  if (request.method === "POST" && url.pathname === "/admin/index")
    return await adminIndex(request, env, new Date());
  if (request.method === "POST" && url.pathname === "/admin/trigger")
    return await adminTrigger(request, env, new Date());
  if (request.method === "POST" && url.pathname === "/admin/cache/purge")
    return await adminCachePurge(request, env);
  if (request.method === "GET" && url.pathname === "/admin/status")
    return await adminStatus(request, env);
  return Response.json({ error: "Not found" }, { status: 404 });
};

const jobQueueName = (job: SyncJob): string => {
  if (job.type === "r2-bucket-jvlink") return JV_RAW_STAGE_QUEUE;
  if (job.type === "r2-bucket-nvlink") return NV_RAW_STAGE_QUEUE;
  if (job.type === "neon-dispatch" || job.type === "neon-table") return NEON_QUEUE;
  return R2_CATALOG_QUEUE;
};

const queueHandler = async (batch: MessageBatch<SyncJob>, env: Env): Promise<void> => {
  for (const message of batch.messages) {
    try {
      if (batch.queue !== jobQueueName(message.body)) throw new PermanentJobError("queue-routing");
      await handleJob(message.body, env);
      message.ack();
    } catch (error: unknown) {
      await recordJobFailure(message.body, env, error);
      console.error(
        JSON.stringify({
          attempt: message.attempts,
          provider: message.body.provider,
          runId: message.body.runId,
          stage: message.body.type,
        }),
      );
      if (isPermanentJobError(error)) message.ack();
      else message.retry({ delaySeconds: Math.min(900, 30 * 2 ** message.attempts) });
    }
  }
};

export default {
  fetch: fetchHandler,
  queue: queueHandler,
  scheduled,
} satisfies ExportedHandler<Env, SyncJob>;

export {
  adminCachePurge,
  adminIndex,
  adminRun,
  adminTrigger,
  adminStatus,
  fetchHandler,
  monitor,
  jobQueueName,
  queueHandler,
  scheduled,
  startAcquisition,
};
