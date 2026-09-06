export type Provider = "jv" | "nv";
export type TriggerKind = "daily" | "manual" | "monitor";

export interface AcquisitionRequest {
  advanceCursor: boolean;
  cursorTime: string;
  fromTime: string;
  provider: Provider;
  runDate: string;
  runId: string;
  toTime: string | null;
}

interface R2BucketJobBase {
  advanceCursor?: boolean;
  cursorTime?: string;
  runDate: string;
  runId: string;
  stagingKey: string;
}

export interface R2BucketJVLinkJob extends R2BucketJobBase {
  type: "r2-bucket-jvlink";
  provider: "jv";
}

export interface R2BucketNVLinkJob extends R2BucketJobBase {
  type: "r2-bucket-nvlink";
  provider: "nv";
}

export type R2BucketJob = R2BucketJVLinkJob | R2BucketNVLinkJob;

export interface CatalogTableJob {
  type: "catalog-table";
  provider: Provider;
  runDate: string;
  runId: string;
  tableName: string;
  tableStagingKey: string;
}

export interface IndexPlanJob {
  type: "index-plan";
  provider: Provider;
  runDate: string;
  runId: string;
  tableName: string;
  partitionValue: string;
}

export interface IndexFileJob {
  type: "index-file";
  provider: Provider;
  runDate: string;
  runId: string;
  tableName: string;
  partitionValue: string;
  chunkId: string;
}

export interface NeonDispatchJob {
  type: "neon-dispatch";
  provider: Provider;
  runDate: string;
  runId: string;
}

export interface NeonTableJob {
  type: "neon-table";
  provider: Provider;
  runDate: string;
  runId: string;
  tableName: string;
  tableStagingKey: string;
}

export interface RecoveryJob {
  type: "recover";
  provider: Provider;
  runDate: string;
  runId: string;
}

export type R2CatalogJob = CatalogTableJob | IndexFileJob | IndexPlanJob | RecoveryJob;
export type NeonJob = NeonDispatchJob | NeonTableJob;
export type SyncJob = NeonJob | R2BucketJob | R2CatalogJob;

export type CatalogColumnType = "int" | "string" | "timestamptz";

export interface RecordColumn {
  catalogType?: CatalogColumnType;
  name: string;
  width: number;
}

export interface RecordLayout {
  columns: readonly RecordColumn[];
  primaryKey: readonly string[];
  recordBytes: number;
  recordType: string;
  tableName: string;
}

export type RecordValue = null | number | string;
export type RecordRow = Readonly<Record<string, RecordValue>>;

export interface TableStage {
  formatVersion: 1;
  provider: Provider;
  records: readonly RecordRow[];
  runId: string;
  tableName: string;
}

export interface RunRow {
  advance_cursor: number;
  catalog_tables: number;
  completed_at: string | null;
  cursor_time: string | null;
  error_stage: string | null;
  files: number;
  from_time: string | null;
  neon_tables: number;
  provider: Provider;
  records: number;
  run_date: string;
  run_id: string;
  staging_key: string | null;
  status: string;
  to_time: string | null;
  updated_at: string;
}

export interface SourceService {
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
}

export interface Env {
  ADMIN_TOKEN: string;
  CATALOG_BUCKET: R2Bucket;
  DB: D1Database;
  NEON_JOBS: Queue<NeonJob>;
  R2_CATALOG_JOBS: Queue<R2CatalogJob>;
  JRA_VAN_WORKER_API_TOKEN: string;
  JV_RAW_STAGE_JOBS: Queue<R2BucketJVLinkJob>;
  JV_SOURCE: SourceService;
  NEON_DATABASE_URL: string;
  NV_RAW_STAGE_JOBS: Queue<R2BucketNVLinkJob>;
  NV_SOURCE: SourceService;
  R2_BUCKET_NAME: string;
  R2_CATALOG_NAMESPACE: string;
  R2_CATALOG_TOKEN: string;
  R2_CATALOG_URI: string;
  R2_CATALOG_WAREHOUSE: string;
  REALTIME_ADMIN_TOKEN: string;
  REALTIME_SYNC: SourceService;
  SOURCE_STAGING: R2Bucket;
  SYNC_CACHE: KVNamespace;
  UMMACON_WORKER_API_TOKEN: string;
}
