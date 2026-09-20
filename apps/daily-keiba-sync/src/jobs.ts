// Runs with bun; deployed as a Cloudflare Worker queue consumer.
import { purgeProviderCursorCache } from "./cache";
import { indexCatalogFile, planCatalogIndex } from "./catalog-index";
import { isPermanentJobError, isTransientJobError } from "./errors";
import { syncCatalogTable } from "./catalog-sync";
import { syncNeonTable } from "./neon-sync";
import { acquisitionBodyForSpec } from "./schedule";
import {
  acquireCatalogLease,
  advanceProviderAcquisitionCursor,
  completeRun,
  getReadyIndexPartitions,
  getRun,
  getRunTable,
  getRunTablePartitions,
  listCatalogTargets,
  invalidateReadyIndexPartitions,
  listRunTables,
  markAcquired,
  markCatalogTable,
  markNeonTable,
  markParsed,
  markTableFailure,
  releaseCatalogLease,
  setRunTableCatalogStatus,
  setRunTableNeonStatus,
  updateRunStatus,
} from "./state";
import {
  createTableStage,
  parseSourceReadable,
  parseTableStage,
  tableStagingKey,
} from "./source-stream";
import { putStream } from "./staging";
import type { CatalogTargetRow } from "./state";
import type {
  AcquisitionRequest,
  CatalogTableJob,
  Env,
  NeonDispatchJob,
  NeonTableJob,
  R2BucketJob,
  R2BucketJVLinkJob,
  R2BucketNVLinkJob,
  RecoveryJob,
  RecordRow,
  RunRow,
  SourceService,
  SyncJob,
  TableStage,
} from "./types";

const NDJSON_CONTENT_TYPE = "application/x-ndjson";

const targetPartition = (target: CatalogTargetRow, record: RecordRow): string => {
  if (target.table_name === "jvd_cs") {
    if (target.partition_field !== "__unpartitioned__")
      throw new Error("Course target requires an unpartitioned configuration");
    return "__all__";
  }
  const value = record[target.partition_field];
  return typeof value === "string" ? value.trim() : "";
};

const rawStagingKey = (job: AcquisitionRequest): string =>
  `source-staging/v1/${job.provider}/${job.runDate}/${job.runId}/source.ndjson`;

const r2BucketJob = (
  provider: AcquisitionRequest["provider"],
  runDate: string,
  runId: string,
  stagingKey: string,
  cursorTime: string,
  advanceCursor: boolean,
): R2BucketJob =>
  provider === "jv"
    ? {
        advanceCursor,
        cursorTime,
        provider: "jv",
        runDate,
        runId,
        stagingKey,
        type: "r2-bucket-jvlink",
      }
    : {
        advanceCursor,
        cursorTime,
        provider: "nv",
        runDate,
        runId,
        stagingKey,
        type: "r2-bucket-nvlink",
      };

const sendR2BucketJob = async (env: Env, job: R2BucketJob): Promise<void> => {
  if (job.type === "r2-bucket-jvlink") await env.JV_RAW_STAGE_JOBS.send(job);
  else await env.NV_RAW_STAGE_JOBS.send(job);
};

const validateAcquisitionIdentity = (
  run: RunRow,
  job: Pick<R2BucketJob, "provider" | "advanceCursor">,
): void => {
  if (run.provider !== job.provider) throw new Error("Source acquisition provider mismatch");
  if (run.data_spec === "COMM" && job.advanceCursor)
    throw new Error("COMM-only acquisition cannot advance the race cursor");
};

const sourceRequest = async (
  job: AcquisitionRequest,
  env: Env,
): Promise<{ request: Request; source: SourceService }> => {
  const run = await getRun(env.DB, job.runId);
  validateAcquisitionIdentity(run, job);
  const body = JSON.stringify(
    acquisitionBodyForSpec({
      provider: job.provider,
      fromTime: job.fromTime,
      toTime: job.toTime,
      dataSpec: run.data_spec,
    }),
  );
  const token = job.provider === "jv" ? env.JRA_VAN_WORKER_API_TOKEN : env.UMMACON_WORKER_API_TOKEN;
  return {
    request: new Request("https://source.internal/acquire/stream", {
      body,
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Length": String(new TextEncoder().encode(body).length),
        "Content-Type": "application/json",
      },
      method: "POST",
    }),
    source: job.provider === "jv" ? env.JV_SOURCE : env.NV_SOURCE,
  };
};

const loadText = async (bucket: R2Bucket, key: string): Promise<string> => {
  const object = await bucket.get(key);
  if (object === null) throw new Error("Staging object not found");
  return await object.text();
};

const loadTableStage = async (
  job: CatalogTableJob | NeonTableJob,
  env: Env,
): Promise<TableStage> => {
  const value: unknown = JSON.parse(await loadText(env.SOURCE_STAGING, job.tableStagingKey));
  const stage = parseTableStage(value);
  if (
    stage.provider !== job.provider ||
    stage.runId !== job.runId ||
    stage.tableName !== job.tableName
  )
    throw new Error("Table staging identity mismatch");
  return stage;
};

const sendJobs = async (queue: Queue<SyncJob>, jobs: readonly SyncJob[]): Promise<void> => {
  if (jobs.length === 0) return;
  await queue.sendBatch(jobs.map((body) => ({ body })));
};

const notifyRealtimeJvRaceDays = async (
  job: CatalogTableJob | NeonDispatchJob | NeonTableJob | RecoveryJob,
  env: Env,
): Promise<void> => {
  if (job.provider !== "jv") return;
  const run = await getRun(env.DB, job.runId);
  if (run.advance_cursor !== 1) return;
  const tables = await listRunTables(env.DB, job.runId);
  if (tables.some((table) => !["succeeded", "not_configured"].includes(table.catalog_status)))
    return;
  const raceTable = tables.find(
    (table) => table.table_name === "jvd_ra" && table.catalog_status === "succeeded",
  );
  if (raceTable === undefined) return;
  // This durable receipt is not a cache: backup retries must not redispatch published days.
  const receiptKey: string = `realtime-notifications/v1/${job.provider}/${job.runId}.json`;
  if ((await env.SOURCE_STAGING.head(receiptKey)) !== null) return;
  const stage = parseTableStage(
    JSON.parse(await loadText(env.SOURCE_STAGING, raceTable.staging_key)) as unknown,
  );
  if (stage.provider !== "jv" || stage.runId !== job.runId || stage.tableName !== "jvd_ra")
    throw new Error("Realtime scheduling stage identity mismatch");
  const dates = new Set<string>();
  for (const record of stage.records) {
    const year = record.kaisai_nen;
    const monthDay = record.kaisai_tsukihi;
    const date = `${typeof year === "string" ? year.trim() : ""}${typeof monthDay === "string" ? monthDay.trim() : ""}`;
    if (!/^20\d{6}$/.test(date)) throw new Error("JV race date is invalid");
    dates.add(date);
  }
  for (const date of dates) {
    const response = await env.REALTIME_SYNC.fetch(
      new Request("https://sync-realtime-data.internal/api/jobs", {
        body: JSON.stringify({ date, type: "discover-urls" }),
        headers: {
          Authorization: `Bearer ${env.REALTIME_ADMIN_TOKEN}`,
          "Content-Type": "application/json",
        },
        method: "POST",
      }),
    );
    if (!response.ok) throw new Error("Realtime scheduling notification failed");
  }
  await env.SOURCE_STAGING.put(
    receiptKey,
    JSON.stringify({ runId: job.runId, dates: [...dates] }),
    {
      httpMetadata: { contentType: "application/json" },
    },
  );
};

const completeSyncedRun = async (
  job: NeonDispatchJob | NeonTableJob,
  env: Env,
  configuredTableCount: number,
  now: Date,
): Promise<void> => {
  await notifyRealtimeJvRaceDays(job, env);
  await completeRun(env.DB, job.runId, "succeeded", configuredTableCount, now);
};

export const acquireRun = async (
  job: AcquisitionRequest,
  env: Env,
  now = new Date(),
): Promise<void> => {
  const { request, source } = await sourceRequest(job, env);
  await updateRunStatus(env.DB, job.runId, "acquiring", now);
  const response = await source.fetch(request);
  if (!response.ok || response.body === null) throw new Error("Source acquisition failed");
  const contentType = response.headers.get("Content-Type") ?? "";
  if (!contentType.startsWith(NDJSON_CONTENT_TYPE)) throw new Error("Invalid source content type");
  const stagingKey = rawStagingKey(job);
  await putStream(env.SOURCE_STAGING, stagingKey, response.body, {
    httpMetadata: { contentType: NDJSON_CONTENT_TYPE },
    customMetadata: { provider: job.provider, runDate: job.runDate, runId: job.runId },
  });
  await markAcquired(env.DB, job.runId, stagingKey, now);
  await sendR2BucketJob(
    env,
    r2BucketJob(
      job.provider,
      job.runDate,
      job.runId,
      stagingKey,
      job.cursorTime,
      job.advanceCursor,
    ),
  );
};

const stageR2Bucket = async (job: R2BucketJob, env: Env, now: Date): Promise<void> => {
  validateAcquisitionIdentity(await getRun(env.DB, job.runId), job);
  await updateRunStatus(env.DB, job.runId, "parsing", now);
  const object = await env.SOURCE_STAGING.get(job.stagingKey);
  if (object === null) throw new Error("Staging object not found");
  const parsed = await parseSourceReadable(object.body, job.provider);
  if (job.advanceCursor) {
    if (job.cursorTime === undefined) throw new Error("R2 Bucket cursor time is missing");
    await advanceProviderAcquisitionCursor(env.DB, job.provider, job.cursorTime, now);
    await purgeProviderCursorCache(env.SYNC_CACHE, job.provider);
  }
  if (parsed.tables.length === 0) {
    await completeRun(env.DB, job.runId, "succeeded_empty", 0, now);
    return;
  }
  const targets = new Map(
    (await listCatalogTargets(env.DB, job.provider)).map((target) => [target.table_name, target]),
  );
  const rows = await Promise.all(
    parsed.tables.map(async (table) => {
      const target = targets.get(table.layout.tableName);
      const partitions =
        target === undefined
          ? []
          : [...new Set(table.records.map((record) => targetPartition(target, record)))];
      if (partitions.some((partition) => partition.length === 0))
        throw new Error("Configured catalog partition is missing");
      const ready =
        target === undefined
          ? new Set<string>()
          : await getReadyIndexPartitions(env.DB, table.layout.tableName, partitions);
      const catalogStatus =
        target === undefined
          ? "not_configured"
          : ready.size === partitions.length
            ? "pending"
            : "index_pending";
      return {
        catalog_status: catalogStatus,
        neon_status: target === undefined ? "not_configured" : "pending",
        partitions,
        source_records: table.records.length,
        staging_key: tableStagingKey(job.provider, job.runDate, job.runId, table.layout.tableName),
        table_name: table.layout.tableName,
      };
    }),
  );
  await Promise.all(
    parsed.tables.map(async (table) => {
      const stagingKey = tableStagingKey(
        job.provider,
        job.runDate,
        job.runId,
        table.layout.tableName,
      );
      await env.SOURCE_STAGING.put(
        stagingKey,
        JSON.stringify(
          createTableStage(job.provider, job.runId, table.layout.tableName, table.records),
        ),
        { httpMetadata: { contentType: "application/json" } },
      );
    }),
  );
  await markParsed(env.DB, job.runId, parsed.files, parsed.records, rows, now);
  const indexJobs: SyncJob[] = rows.flatMap((row) =>
    row.catalog_status === "index_pending"
      ? row.partitions.map(
          (partitionValue) =>
            ({
              partitionValue,
              provider: job.provider,
              runDate: job.runDate,
              runId: job.runId,
              tableName: row.table_name,
              type: "index-plan",
            }) satisfies SyncJob,
        )
      : [],
  );
  const catalogJobs: SyncJob[] = rows
    .filter((row) => row.catalog_status === "pending")
    .map(
      (row) =>
        ({
          provider: job.provider,
          runDate: job.runDate,
          runId: job.runId,
          tableName: row.table_name,
          tableStagingKey: row.staging_key,
          type: "catalog-table",
        }) satisfies SyncJob,
    );
  await sendJobs(env.R2_CATALOG_JOBS, [...indexJobs, ...catalogJobs]);
  if (indexJobs.length === 0 && catalogJobs.length === 0)
    await completeRun(env.DB, job.runId, "succeeded", 0, now);
};

const stageR2BucketJVLink = async (job: R2BucketJVLinkJob, env: Env, now: Date): Promise<void> =>
  await stageR2Bucket(job, env, now);

const stageR2BucketNVLink = async (job: R2BucketNVLinkJob, env: Env, now: Date): Promise<void> =>
  await stageR2Bucket(job, env, now);

const rebuildCatalogIndexes = async (
  job: CatalogTableJob,
  env: Env,
  partitions: readonly string[],
  now: Date,
): Promise<void> => {
  await setRunTableCatalogStatus(env.DB, job.runId, job.tableName, "index_pending", now);
  await sendJobs(
    env.R2_CATALOG_JOBS,
    partitions.map((partitionValue) => ({
      partitionValue,
      provider: job.provider,
      runDate: job.runDate,
      runId: job.runId,
      tableName: job.tableName,
      type: "index-plan",
    })),
  );
};

const publishCatalogRun = async (job: CatalogTableJob, env: Env, now: Date): Promise<void> => {
  const tables = await listRunTables(env.DB, job.runId);
  if (
    !tables.every(
      (candidate) =>
        candidate.catalog_status === "succeeded" || candidate.catalog_status === "not_configured",
    )
  )
    return;
  await notifyRealtimeJvRaceDays(job, env);
  const run = await getRun(env.DB, job.runId);
  if (run.status === "succeeded") return;
  await updateRunStatus(env.DB, job.runId, "catalog_succeeded", now);
  await env.NEON_JOBS.send({
    provider: job.provider,
    runDate: job.runDate,
    runId: job.runId,
    type: "neon-dispatch",
  });
};

const syncCatalog = async (job: CatalogTableJob, env: Env, now: Date): Promise<void> => {
  const table = await getRunTable(env.DB, job.runId, job.tableName);
  if (table.catalog_status === "succeeded") {
    await publishCatalogRun(job, env, now);
    return;
  }
  const partitions = await getRunTablePartitions(env.DB, job.runId, job.tableName);
  const ready = await getReadyIndexPartitions(env.DB, job.tableName, partitions);
  if (ready.size !== partitions.length) {
    await rebuildCatalogIndexes(job, env, partitions, now);
    return;
  }
  const leaseOwner = `${job.runId}:${crypto.randomUUID()}`;
  if (!(await acquireCatalogLease(env.DB, job.tableName, leaseOwner, now)))
    throw new Error("Catalog table lease is held");
  try {
    const result = await syncCatalogTable(await loadTableStage(job, env), env, now);
    await markCatalogTable(
      env.DB,
      job.runId,
      job.tableName,
      result.snapshotId,
      result.deletedRows,
      now,
    );
  } catch (error: unknown) {
    if (!isTransientJobError(error) || error.safeStage !== "catalog-index-stale") throw error;
    await invalidateReadyIndexPartitions(env.DB, job.tableName, partitions, now);
    await rebuildCatalogIndexes(job, env, partitions, now);
    return;
  } finally {
    await releaseCatalogLease(env.DB, job.tableName, leaseOwner);
  }
  await publishCatalogRun(job, env, now);
};

const dispatchNeon = async (job: NeonDispatchJob, env: Env, now: Date): Promise<void> => {
  const tables = await listRunTables(env.DB, job.runId);
  if (
    tables.length === 0 ||
    tables.some(
      (table) => table.catalog_status !== "succeeded" && table.catalog_status !== "not_configured",
    )
  )
    throw new Error("Neon dispatch attempted before R2 Catalog completion");
  // Resume publication for messages enqueued before Catalog-first notification was deployed.
  await notifyRealtimeJvRaceDays(job, env);
  const configured = tables.filter((table) => table.catalog_status === "succeeded");
  const pending = configured.filter((table) => table.neon_status !== "succeeded");
  if (pending.length === 0) {
    await completeSyncedRun(job, env, configured.length, now);
    return;
  }
  await updateRunStatus(env.DB, job.runId, "neon_pending", now);
  await sendJobs(
    env.NEON_JOBS,
    pending.map((table) => ({
      provider: job.provider,
      runDate: job.runDate,
      runId: job.runId,
      tableName: table.table_name,
      tableStagingKey: table.staging_key,
      type: "neon-table",
    })),
  );
};

const syncNeon = async (job: NeonTableJob, env: Env, now: Date): Promise<void> => {
  const tablesBefore = await listRunTables(env.DB, job.runId);
  if (
    tablesBefore.some(
      (table) => table.catalog_status !== "succeeded" && table.catalog_status !== "not_configured",
    )
  )
    throw new Error("Neon write attempted before R2 Catalog completion");
  const tableBefore = tablesBefore.find((table) => table.table_name === job.tableName);
  if (tableBefore === undefined || tableBefore.catalog_status !== "succeeded")
    throw new Error("Neon backup requires a committed Catalog table");
  if (tableBefore.neon_status !== "succeeded") {
    await syncNeonTable(await loadTableStage(job, env), env);
    await markNeonTable(env.DB, job.runId, job.tableName, now);
  }
  const tables = await listRunTables(env.DB, job.runId);
  const configured = tables.filter((table) => table.catalog_status === "succeeded");
  if (configured.every((table) => table.neon_status === "succeeded"))
    await completeSyncedRun(job, env, configured.length, now);
};

const recover = async (job: RecoveryJob, env: Env, now: Date): Promise<void> => {
  const run = await getRun(env.DB, job.runId);
  if (run.status === "succeeded" || run.status === "succeeded_empty") return;
  if (run.staging_key === null) throw new Error("Recovery requires scheduled acquisition");
  const tables = await listRunTables(env.DB, job.runId);
  if (tables.length === 0) {
    if (run.advance_cursor === 1 && run.cursor_time === null)
      throw new Error("Recovery cursor time is missing");
    await sendR2BucketJob(
      env,
      r2BucketJob(
        job.provider,
        job.runDate,
        job.runId,
        run.staging_key,
        run.cursor_time ?? "",
        run.advance_cursor === 1,
      ),
    );
    return;
  }
  const indexJobs: SyncJob[] = [];
  for (const table of tables.filter((candidate) => candidate.catalog_status === "index_pending")) {
    const partitions = await getRunTablePartitions(env.DB, job.runId, table.table_name);
    // Re-send ready partitions too: index-plan wakes waiters after a lost
    // completion notification, rather than incorrectly dispatching Neon.
    for (const partitionValue of partitions) {
      indexJobs.push({
        partitionValue,
        provider: job.provider,
        runDate: job.runDate,
        runId: job.runId,
        tableName: table.table_name,
        type: "index-plan",
      });
    }
  }
  const catalogJobs: SyncJob[] = tables
    .filter((table) => table.catalog_status === "pending" || table.catalog_status === "failed")
    .map((table) => ({
      provider: job.provider,
      runDate: job.runDate,
      runId: job.runId,
      tableName: table.table_name,
      tableStagingKey: table.staging_key,
      type: "catalog-table",
    }));
  await notifyRealtimeJvRaceDays(job, env);
  if (
    tables.some(
      (table) =>
        table.catalog_status === "failed_permanent" || table.neon_status === "failed_permanent",
    )
  )
    return;
  if (indexJobs.length > 0 || catalogJobs.length > 0) {
    await sendJobs(env.R2_CATALOG_JOBS, [...indexJobs, ...catalogJobs]);
    return;
  }
  await env.NEON_JOBS.send({
    provider: job.provider,
    runDate: job.runDate,
    runId: job.runId,
    type: "neon-dispatch",
  });
  await updateRunStatus(env.DB, job.runId, "recovery_queued", now);
};

export const handleJob = async (job: SyncJob, env: Env, now = new Date()): Promise<void> => {
  if (job.type === "r2-bucket-jvlink") return await stageR2BucketJVLink(job, env, now);
  if (job.type === "r2-bucket-nvlink") return await stageR2BucketNVLink(job, env, now);
  if (job.type === "catalog-table") return await syncCatalog(job, env, now);
  if (job.type === "index-plan") return await planCatalogIndex(job, env, now);
  if (job.type === "index-file") return await indexCatalogFile(job, env, now);
  if (job.type === "neon-dispatch") return await dispatchNeon(job, env, now);
  if (job.type === "neon-table") return await syncNeon(job, env, now);
  return await recover(job, env, now);
};

export const recordJobFailure = async (
  job: SyncJob,
  env: Env,
  error: unknown,
  now = new Date(),
): Promise<void> => {
  const permanent = isPermanentJobError(error);
  if (job.type === "catalog-table" || job.type === "neon-table") {
    const stage = job.type === "catalog-table" ? "catalog" : "neon";
    const table = await getRunTable(env.DB, job.runId, job.tableName);
    const committed: boolean =
      job.type === "catalog-table"
        ? table.catalog_status === "succeeded"
        : table.neon_status === "succeeded";
    if (committed) {
      // Downstream publication failure cannot undo a durable data commit.
      await updateRunStatus(env.DB, job.runId, "publication_failed", now, "publication");
      return;
    }
    if (permanent) {
      if (job.type === "catalog-table")
        await setRunTableCatalogStatus(env.DB, job.runId, job.tableName, "failed_permanent", now);
      else await setRunTableNeonStatus(env.DB, job.runId, job.tableName, "failed_permanent", now);
      await updateRunStatus(env.DB, job.runId, `${stage}_failed`, now, error.safeStage);
    } else {
      await markTableFailure(env.DB, job.runId, job.tableName, stage, now);
      if (isTransientJobError(error))
        await updateRunStatus(env.DB, job.runId, `${stage}_failed`, now, error.safeStage);
    }
    return;
  }
  const stage = permanent ? error.safeStage : job.type;
  if (permanent && (job.type === "index-plan" || job.type === "index-file"))
    await setRunTableCatalogStatus(env.DB, job.runId, job.tableName, "failed_permanent", now);
  await updateRunStatus(env.DB, job.runId, `${job.type}_failed`, now, stage);
};
