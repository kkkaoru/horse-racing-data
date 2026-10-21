// Runs in Workers; verification runs with bun package scripts.
// Appends the production running-style predictions to the R2 Catalog table
// `race_running_style_model_predictions`, so training reads them from the Catalog
// instead of Neon. Source: the per-day per-model Parquet that sync-realtime-data
// exports from D1 to `pc-keiba-features-archive` (a full-day snapshot, rewritten
// after every race). The etag of each appended object is kept in KV; an object whose
// etag changed is appended whole. predicted_at is the job's planned time, not its
// write time, so a timestamp watermark would drop late-landing rows for good.
// The table is append-only: readers keep the newest prediction_generated_at per
// key (finish-position `_catalog_attach.py`), and `backfill_running_style_catalog.py`
// compacts it offline.
import { icebergTransaction } from "icebird";
import { parquetReadObjects } from "hyparquet";
import { compressors } from "hyparquet-compressors";
import { connectCatalog } from "./catalog-sync";
import { createR2Resolver } from "./r2-resolver";
import type { Env } from "./types";

export type RunningStyleCatalogEnvironment = Pick<
  Env,
  | "CATALOG_BUCKET"
  | "FEATURES_ARCHIVE"
  | "R2_BUCKET_NAME"
  | "R2_CATALOG_NAMESPACE"
  | "R2_CATALOG_TOKEN"
  | "R2_CATALOG_URI"
  | "R2_CATALOG_WAREHOUSE"
  | "SYNC_CACHE"
>;

export interface RunningStyleCatalogRecord {
  model_version: string;
  source: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  ketto_toroku_bango: string;
  umaban: number;
  p_nige: number;
  p_senkou: number;
  p_sashi: number;
  p_oikomi: number;
  predicted_class: number;
  predicted_label: string;
  cell_model_key: string | null;
  cell_variant_id: string | null;
  predicted_corner_front_score: null;
  predicted_corner_rank: null;
  prediction_generated_at: Date;
}

export interface RunningStyleObjectRef {
  etag: string;
  key: string;
}

export interface RunningStyleCatalogSyncResult {
  appendedRows: number;
  objects: number;
  snapshotId: string | null;
}

export const RUNNING_STYLE_CATALOG_TABLE = "race_running_style_model_predictions";
export const RUNNING_STYLE_PARQUET_PREFIX = "running-style/predictions/by-day/raw-iceberg-v1/";
export const RUNNING_STYLE_CATALOG_LOOKBACK_DAYS = 3;
// Read by pipeline-health-monitor to alert when the daily sync stops succeeding.
export const RUNNING_STYLE_CATALOG_HEALTH_KEY = "running-style-catalog:last-success";
// v2 markers hold object etags (v1 held predicted_at watermarks and is ignored).
const MARKER_KEY_PREFIX = "running-style-catalog:v2:";
const MS_PER_DAY = 86_400_000;
const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const requireString = (row: Record<string, unknown>, column: string): string => {
  const value = row[column];
  if (typeof value !== "string" || value.length === 0)
    throw new Error(`Running-style Parquet row has an invalid ${column}`);
  return value;
};

const optionalString = (row: Record<string, unknown>, column: string): string | null =>
  row[column] === null || row[column] === undefined ? null : requireString(row, column);

const requireFinite = (row: Record<string, unknown>, column: string): number => {
  const value = row[column];
  if (typeof value !== "number" || !Number.isFinite(value))
    throw new Error(`Running-style Parquet row has an invalid ${column}`);
  return value;
};

const requireInteger = (row: Record<string, unknown>, column: string): number => {
  const value = requireFinite(row, column);
  if (!Number.isInteger(value))
    throw new Error(`Running-style Parquet row has an invalid ${column}`);
  return value;
};

const requireTimestamp = (row: Record<string, unknown>): Date => {
  const timestamp = new Date(requireString(row, "predicted_at"));
  if (Number.isNaN(timestamp.getTime()))
    throw new Error("Running-style Parquet row has an invalid predicted_at");
  return timestamp;
};

export const toRunningStyleCatalogRecord = (value: unknown): RunningStyleCatalogRecord => {
  if (!isObject(value)) throw new Error("Running-style Parquet row is not an object");
  return {
    model_version: requireString(value, "model_version"),
    source: requireString(value, "source"),
    kaisai_nen: requireString(value, "kaisai_nen"),
    kaisai_tsukihi: requireString(value, "kaisai_tsukihi"),
    keibajo_code: requireString(value, "keibajo_code"),
    race_bango: requireString(value, "race_bango"),
    ketto_toroku_bango: requireString(value, "ketto_toroku_bango"),
    predicted_label: requireString(value, "predicted_label"),
    p_nige: requireFinite(value, "p_nige"),
    p_senkou: requireFinite(value, "p_senkou"),
    p_sashi: requireFinite(value, "p_sashi"),
    p_oikomi: requireFinite(value, "p_oikomi"),
    umaban: requireInteger(value, "umaban"),
    predicted_class: requireInteger(value, "predicted_class"),
    cell_model_key: optionalString(value, "cell_model_key"),
    cell_variant_id: optionalString(value, "cell_variant_id"),
    predicted_corner_front_score: null,
    predicted_corner_rank: null,
    prediction_generated_at: requireTimestamp(value),
  };
};

const formatDayPrefix = (timestamp: number): string => {
  const iso = new Date(timestamp).toISOString();
  return `${RUNNING_STYLE_PARQUET_PREFIX}${iso.slice(0, 4)}/${iso.slice(5, 7)}/${iso.slice(8, 10)}/`;
};

// The lookback days before runDate (JST YYYYMMDD), oldest first. The run date is
// left out because its races (and so its Parquet) are not finished yet.
export const runningStyleDayPrefixes = (runDate: string, lookbackDays: number): string[] => {
  const end =
    Date.UTC(
      Number(runDate.slice(0, 4)),
      Number(runDate.slice(4, 6)) - 1,
      Number(runDate.slice(6, 8)),
    ) - MS_PER_DAY;
  return Array.from({ length: lookbackDays }, (_, index) =>
    formatDayPrefix(end - (lookbackDays - 1 - index) * MS_PER_DAY),
  );
};

const listParquetObjects = async (
  bucket: R2Bucket,
  prefix: string,
): Promise<RunningStyleObjectRef[]> => {
  const listed = await bucket.list({ prefix });
  if (listed.truncated) throw new Error(`Running-style Parquet listing is truncated: ${prefix}`);
  return listed.objects
    .filter((object) => object.key.endsWith(".parquet"))
    .map((object) => ({ etag: object.etag, key: object.key }));
};

const isChanged = async (
  env: RunningStyleCatalogEnvironment,
  object: RunningStyleObjectRef,
): Promise<boolean> =>
  (await env.SYNC_CACHE.get(`${MARKER_KEY_PREFIX}${object.key}`)) !== object.etag;

export const readObjectRecords = async (
  env: RunningStyleCatalogEnvironment,
  key: string,
): Promise<RunningStyleCatalogRecord[]> => {
  const object = await env.FEATURES_ARCHIVE.get(key);
  if (object === null) throw new Error(`Running-style Parquet object disappeared: ${key}`);
  const rows: readonly unknown[] = await parquetReadObjects({
    compressors,
    file: await object.arrayBuffer(),
  });
  return rows.map(toRunningStyleCatalogRecord);
};

const appendRecords = async (
  env: RunningStyleCatalogEnvironment,
  records: readonly RunningStyleCatalogRecord[],
): Promise<string> => {
  const committed = await icebergTransaction(
    {
      catalog: await connectCatalog(env),
      namespace: env.R2_CATALOG_NAMESPACE,
      resolver: createR2Resolver(env.CATALOG_BUCKET, env.R2_BUCKET_NAME),
      table: RUNNING_STYLE_CATALOG_TABLE,
    },
    async (transaction) => {
      await transaction.append({ records: [...records] });
    },
  );
  const snapshotId = committed["current-snapshot-id"];
  if (snapshotId === undefined) throw new Error("Iceberg commit returned no current snapshot");
  return String(snapshotId);
};

// A crash between the commit and the marker writes re-appends the same objects on
// the next run; readers de-duplicate by key, so that is harmless. An object rewritten
// between the listing and the read is appended at its newer content and re-appended
// on the next run because its marker holds the listed etag.
const appendChangedObjects = async (
  env: RunningStyleCatalogEnvironment,
  runDate: string,
): Promise<RunningStyleCatalogSyncResult> => {
  const objects = (
    await Promise.all(
      runningStyleDayPrefixes(runDate, RUNNING_STYLE_CATALOG_LOOKBACK_DAYS).map((prefix) =>
        listParquetObjects(env.FEATURES_ARCHIVE, prefix),
      ),
    )
  ).flat();
  const flags = await Promise.all(objects.map((object) => isChanged(env, object)));
  const changed = objects.filter((_, index) => flags[index]);
  const records = (
    await Promise.all(changed.map((object) => readObjectRecords(env, object.key)))
  ).flat();
  if (records.length === 0) return { appendedRows: 0, objects: objects.length, snapshotId: null };
  const snapshotId = await appendRecords(env, records);
  await Promise.all(
    changed.map((object) => env.SYNC_CACHE.put(`${MARKER_KEY_PREFIX}${object.key}`, object.etag)),
  );
  return { appendedRows: records.length, objects: objects.length, snapshotId };
};

export const syncRunningStyleCatalog = async (
  env: RunningStyleCatalogEnvironment,
  runDate: string,
  now: Date,
): Promise<RunningStyleCatalogSyncResult> => {
  const result = await appendChangedObjects(env, runDate);
  await env.SYNC_CACHE.put(
    RUNNING_STYLE_CATALOG_HEALTH_KEY,
    JSON.stringify({ at: now.toISOString(), ...result }),
  );
  return result;
};
