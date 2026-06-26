// Run via bun (CLI scripts/push-neon-sync.ts), shared helpers in this module.
import { createHash } from "node:crypto";

export type TableMetadata = {
  tableName: string;
  estimatedRows: number;
  estimatedBytes: number;
  columnList: string;
  primaryKeyList: string;
  primaryKeyJoin: string;
  updateList: string;
};

export type StagePrefixKind = "full" | "incremental" | "reincremental";

export interface BuildStageTableNameInput {
  kind: StagePrefixKind;
  pid: number;
  tableName: string;
}

export type DependencyEdge = {
  childTable: string;
  parentTable: string;
};

export type SyncConcurrency = number | "auto";

export type SyncStrategy = "timestamp-incremental" | "pk-incremental" | "full-replace";

export interface TableProfile {
  tableName: string;
  rowCount: number;
  hasUpdateChurn: boolean;
  timestampColumn: string | null;
  hasPrimaryKey: boolean;
  strategy: SyncStrategy;
}

export interface SyncStrategyThresholds {
  smallTableMaxRows: number;
  updateChurnMinTuples: number;
}

export type PushSyncConfig = {
  concurrency: SyncConcurrency;
  deleteMissingRows: boolean;
  applyMode: "replace" | "upsert";
  neonConnectTimeoutSeconds: number;
  neonConnectRetrySeconds: number;
  selectedTables?: string[];
  strategyMode: "auto" | "full";
  strategyThresholds: SyncStrategyThresholds;
};

const DEFAULT_SMALL_TABLE_MAX_ROWS = 10000;
const DEFAULT_UPDATE_CHURN_MIN_TUPLES = 1000;
// PostgreSQL NAMEDATALEN limit minus the worst-case derived identifier suffix
// ("_pk" added by buildNeonApplySql for the stage primary key index).
// Stage table names longer than this collide with their own derived index name
// after PG silently truncates identifiers > 63 chars.
const POSTGRES_NAMEDATALEN_LIMIT = 63;
const STAGE_DERIVED_INDEX_SUFFIX_LENGTH = 3;
const MAX_STAGE_NAME_LENGTH = POSTGRES_NAMEDATALEN_LIMIT - STAGE_DERIVED_INDEX_SUFFIX_LENGTH;
const STAGE_HASH_LENGTH = 8;
const STAGE_NAME_INVALID_CHAR_PATTERN = /[^A-Za-z0-9_]/g;
const STAGE_NAME_INVALID_CHAR_REPLACEMENT = "_";
const STAGE_PREFIX_BY_KIND: Record<StagePrefixKind, string> = {
  full: "replica_sync_stage",
  incremental: "replica_sync_stage_inc",
  reincremental: "replica_sync_stage_reinc",
};
const TIMESTAMP_COLUMN_PRIORITY: readonly string[] = [
  "updated_at",
  "update_timestamp",
  "data_sakusei_nengappi",
  "prediction_generated_at",
  "evaluated_at",
  "activated_at",
  "modified_at",
  "generated_at",
];
const INCLUSIVE_INCREMENTAL_TIMESTAMP_COLUMNS = new Set(["data_sakusei_nengappi"]);

export type ProgressEvent =
  | {
      type: "start";
      totalTables: number;
      totalEstimatedRows: number;
      dependencyLevels: number;
      concurrency: SyncConcurrency;
    }
  | {
      type: "level-start";
      dependencyLevel: number;
      levelTables: number;
      levelEstimatedRows: number;
      concurrency: number;
    }
  | {
      type: "neon-wait-start";
      timeoutSeconds: number;
      retrySeconds: number;
    }
  | {
      type: "neon-wait-retry";
      elapsedSeconds: number;
      retrySeconds: number;
    }
  | {
      type: "neon-ready";
      elapsedSeconds: number;
    }
  | {
      type: "table-start";
      tableName: string;
      estimatedRows: number;
      dependencyLevel: number;
      levelConcurrency: number;
      runningTables: number;
      runningTableNames: string[];
      completedTables: number;
      completedTableNames: string[];
      remainingTables: number;
      remainingTableNames: string[];
      syncedEstimatedRows: number;
      remainingEstimatedRows: number;
      elapsedSeconds: number;
      etaSeconds: number;
    }
  | {
      type: "table-done";
      tableName: string;
      estimatedRows: number;
      dependencyLevel: number;
      levelConcurrency: number;
      tableElapsedSeconds: number;
      runningTables: number;
      runningTableNames: string[];
      completedTables: number;
      completedTableNames: string[];
      totalTables: number;
      syncedEstimatedRows: number;
      totalEstimatedRows: number;
      remainingTables: number;
      remainingTableNames: string[];
      remainingEstimatedRows: number;
      elapsedSeconds: number;
      etaSeconds: number;
    }
  | {
      type: "chunk-plan";
      tableName: string;
      rowCount: number;
      chunkCount: number;
      chunkRows: number;
      wallClockTimeoutSeconds: number;
      idleTimeoutSeconds: number;
    }
  | {
      type: "chunk-done";
      tableName: string;
      chunkIndex: number;
      chunkCount: number;
      rowsDone: number;
      rowsTotal: number;
      chunkElapsedSeconds: number;
      tableElapsedSeconds: number;
      rowsPerSecond: number;
      etaTableSeconds: number;
    }
  | {
      type: "timeout-warning";
      tableName: string;
      label: string;
      elapsedSeconds: number;
      timeoutSeconds: number;
      kind: "idle" | "wall-clock";
    }
  | {
      type: "complete";
      totalTables: number;
      totalEstimatedRows: number;
      elapsedSeconds: number;
    };

export type PushSyncDependencies = {
  nowSeconds: () => number;
  sleep: (milliseconds: number) => Promise<void>;
  checkNeonReady: () => Promise<boolean>;
  syncTable: (table: TableMetadata) => Promise<void>;
  report: (event: ProgressEvent) => void;
};

export function parsePositiveInteger(value: string | undefined, fallback: number): number {
  if (value === undefined || value.trim() === "") {
    return fallback;
  }

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }

  return parsed;
}

export function parseConcurrency(value: string | undefined): SyncConcurrency {
  if (value === undefined || value.trim() === "" || value.trim().toLowerCase() === "auto") {
    return "auto";
  }

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return "auto";
  }

  return parsed;
}

export function parseBoolean(value: string | undefined, fallback: boolean): boolean {
  if (value === undefined || value.trim() === "") {
    return fallback;
  }

  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }

  return fallback;
}

export function parseApplyMode(value: string | undefined): "replace" | "upsert" {
  const normalized = value?.trim().toLowerCase();
  if (normalized === "upsert") {
    return "upsert";
  }
  return "replace";
}

export function parseSelectedTables(value: string | undefined): string[] | undefined {
  const tables = value
    ?.split(",")
    .map((table) => table.trim())
    .filter((table) => table.length > 0);

  return tables && tables.length > 0 ? tables : undefined;
}

export function parseStrategyMode(value: string | undefined): "auto" | "full" {
  if (value === undefined) return "auto";
  const normalized = value.trim().toLowerCase();
  if (normalized === "full" || normalized === "force-full") return "full";
  return "auto";
}

export function buildConfig(env: Record<string, string | undefined>): PushSyncConfig {
  return {
    concurrency: parseConcurrency(env.REPLICA_SYNC_CONCURRENCY),
    deleteMissingRows: parseBoolean(env.REPLICA_SYNC_DELETE, true),
    applyMode: parseApplyMode(env.REPLICA_SYNC_APPLY_MODE),
    neonConnectTimeoutSeconds: parsePositiveInteger(env.NEON_CONNECT_TIMEOUT_SECONDS, 120),
    neonConnectRetrySeconds: parsePositiveInteger(env.NEON_CONNECT_RETRY_SECONDS, 5),
    selectedTables: parseSelectedTables(env.REPLICA_SYNC_TABLES),
    strategyMode: parseStrategyMode(env.REPLICA_SYNC_STRATEGY),
    strategyThresholds: {
      smallTableMaxRows: parsePositiveInteger(
        env.REPLICA_SYNC_SMALL_TABLE_MAX_ROWS,
        DEFAULT_SMALL_TABLE_MAX_ROWS,
      ),
      updateChurnMinTuples: parsePositiveInteger(
        env.REPLICA_SYNC_UPDATE_CHURN_MIN_TUPLES,
        DEFAULT_UPDATE_CHURN_MIN_TUPLES,
      ),
    },
  };
}

export function buildTableProfileSql(selectedTables: string[] | undefined): string {
  const tableFilterSql = buildTableFilterSql(selectedTables);
  const priorityList = TIMESTAMP_COLUMN_PRIORITY.map((name) => quoteLiteral(name)).join(",");
  return `
with candidate as (
  select c.relname as table_name,
    greatest(0, c.reltuples::bigint) as row_count,
    c.oid
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where c.relkind = 'r'
    and n.nspname = 'public'
    and ${tableFilterSql}
),
churn as (
  select s.relname as table_name, coalesce(s.n_tup_upd, 0)::bigint as n_tup_upd
  from pg_stat_user_tables s
),
ts_cols as (
  select c.table_name,
    col.column_name,
    array_position(array[${priorityList}], col.column_name) as priority
  from information_schema.columns col
  join candidate c on c.table_name = col.table_name
  where col.table_schema = 'public'
    and col.column_name = any(array[${priorityList}])
),
pk_present as (
  select c.relname as table_name, true as has_pk
  from pg_index i
  join pg_class c on c.oid = i.indrelid
  join pg_namespace n on n.oid = c.relnamespace
  where i.indisprimary
    and n.nspname = 'public'
)
select
  c.table_name,
  coalesce(c.row_count, 0) as row_count,
  coalesce(ch.n_tup_upd, 0) as n_tup_upd,
  coalesce(pk.has_pk, false) as has_pk,
  (
    select tc.column_name from ts_cols tc
    where tc.table_name = c.table_name
    order by tc.priority asc nulls last
    limit 1
  ) as timestamp_column
from candidate c
left join churn ch on ch.table_name = c.table_name
left join pk_present pk on pk.table_name = c.table_name
order by c.table_name
`;
}

export function parseTableProfiles(
  output: string,
  thresholds: SyncStrategyThresholds,
  mode: "auto" | "full" = "auto",
): TableProfile[] {
  const lines = output
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line !== "");
  return lines.map((line) => parseTableProfileLine(line, thresholds, mode));
}

function parseTableProfileLine(
  line: string,
  thresholds: SyncStrategyThresholds,
  mode: "auto" | "full",
): TableProfile {
  const [tableName = "", rowCountText = "0", nTupUpdText = "0", hasPkText = "f", tsColumn = ""] =
    line.split("\t");
  const rowCount = Number(rowCountText);
  const nTupUpd = Number(nTupUpdText);
  const hasPrimaryKey = hasPkText === "t";
  const timestampColumn = tsColumn === "" ? null : tsColumn;
  const hasUpdateChurn = nTupUpd >= thresholds.updateChurnMinTuples;
  const strategy = resolveStrategy({
    rowCount,
    hasUpdateChurn,
    timestampColumn,
    hasPrimaryKey,
    thresholds,
    mode,
  });
  return {
    tableName,
    rowCount,
    hasUpdateChurn,
    timestampColumn,
    hasPrimaryKey,
    strategy,
  };
}

interface ResolveStrategyInput {
  rowCount: number;
  hasUpdateChurn: boolean;
  timestampColumn: string | null;
  hasPrimaryKey: boolean;
  thresholds: SyncStrategyThresholds;
  mode: "auto" | "full";
}

export function resolveStrategy(input: ResolveStrategyInput): SyncStrategy {
  if (input.mode === "full") return "full-replace";
  if (!input.hasPrimaryKey) return "full-replace";
  if (input.rowCount <= input.thresholds.smallTableMaxRows) return "full-replace";
  if (input.hasUpdateChurn) {
    return input.timestampColumn !== null ? "timestamp-incremental" : "full-replace";
  }
  if (input.timestampColumn !== null) return "timestamp-incremental";
  return "pk-incremental";
}

const UNKNOWN_PROFILE_SKIP_REASON =
  "skipped — no sync profile (strategy=unknown). Inspect loadTableProfileMap to add support.";

export interface SkipUnknownDecision {
  skip: boolean;
  reason?: string;
}

export interface DecideSkipForUnknownProfileInput {
  profile: TableProfile | undefined;
}

export function decideSkipForUnknownProfile(
  input: DecideSkipForUnknownProfileInput,
): SkipUnknownDecision {
  if (input.profile === undefined) {
    return { skip: true, reason: UNKNOWN_PROFILE_SKIP_REASON };
  }
  return { skip: false };
}

export function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

function computeShortStageHash(input: string): string {
  return createHash("sha1").update(input).digest("hex").slice(0, STAGE_HASH_LENGTH);
}

// Compose a deterministic per-process stage table name guaranteed to fit
// within MAX_STAGE_NAME_LENGTH so derived identifiers (e.g. "<stage>_pk")
// stay below PostgreSQL's NAMEDATALEN limit. Long source-table names are
// truncated and suffixed with a short sha1 hash for uniqueness.
export function buildStageTableName(input: BuildStageTableNameInput): string {
  const sanitized = input.tableName.replaceAll(
    STAGE_NAME_INVALID_CHAR_PATTERN,
    STAGE_NAME_INVALID_CHAR_REPLACEMENT,
  );
  const prefix = STAGE_PREFIX_BY_KIND[input.kind];
  const fixedHead = `${prefix}_${input.pid}_`;
  const base = `${fixedHead}${sanitized}`;
  if (base.length <= MAX_STAGE_NAME_LENGTH) return base;
  const hash = computeShortStageHash(sanitized);
  const reservedTail = `_${hash}`;
  const truncatedTableLength = Math.max(
    MAX_STAGE_NAME_LENGTH - fixedHead.length - reservedTail.length,
    0,
  );
  const truncatedTable = sanitized.slice(0, truncatedTableLength);
  return `${fixedHead}${truncatedTable}${reservedTail}`;
}

export function buildFingerprintSql(table: TableMetadata): string {
  const pkExpression = `(${table.primaryKeyList})::text`;
  return `select count(*)::text || E'\\t' || coalesce(max(${pkExpression}), '') from public.${quoteIdentifier(table.tableName)}`;
}

export function buildTimestampFingerprintSql(table: TableMetadata, tsColumn: string): string {
  return `select count(*)::text || E'\\t' || coalesce(max(${quoteIdentifier(tsColumn)})::text, '') from public.${quoteIdentifier(table.tableName)}`;
}

export interface FingerprintResult {
  count: number;
  marker: string;
}

export function parseFingerprintLine(line: string): FingerprintResult {
  const [countText = "", marker = ""] = line.trim().split("\t");
  return {
    count: Number(countText),
    marker,
  };
}

export function buildIncrementalCopyFromSql(
  table: TableMetadata,
  options: { keyExpression: string; neonMarker: string; comparator: ">" | ">=" },
): string {
  const sanitizedMarker = options.neonMarker.replaceAll("'", "''");
  const where =
    sanitizedMarker === ""
      ? ""
      : `where ${options.keyExpression} ${options.comparator} '${sanitizedMarker}'`;
  return `COPY (SELECT ${table.columnList} FROM public.${quoteIdentifier(table.tableName)} ${where}) TO STDOUT WITH (FORMAT csv, NULL '\\N');`;
}

export function pkExpression(table: TableMetadata): string {
  return `(${table.primaryKeyList})::text`;
}

export function timestampKeyExpression(tsColumn: string): string {
  return `(${quoteIdentifier(tsColumn)})::text`;
}

export function incrementalComparatorForTimestampColumn(tsColumn: string | null): ">" | ">=" {
  return tsColumn !== null && INCLUSIVE_INCREMENTAL_TIMESTAMP_COLUMNS.has(tsColumn) ? ">=" : ">";
}

export function shouldRefreshInclusiveIncrementalMarker(tsColumn: string | null): boolean {
  return tsColumn !== null && INCLUSIVE_INCREMENTAL_TIMESTAMP_COLUMNS.has(tsColumn);
}

export function buildIncrementalApplySql(
  table: TableMetadata,
  stageTableName = "replica_sync_stage_inc",
  temporaryStage = true,
): {
  preCopySql: string;
  copySql: string;
  postCopySql: string;
  cleanupSql: string;
} {
  const quotedTableName = quoteIdentifier(table.tableName);
  const quotedStageTableName = quoteIdentifier(stageTableName);
  const stageTableReference = temporaryStage
    ? quotedStageTableName
    : `public.${quotedStageTableName}`;
  const stageCreateSql = temporaryStage
    ? `CREATE TEMP TABLE ${quotedStageTableName} (LIKE public.${quotedTableName} INCLUDING DEFAULTS) ON COMMIT DROP;`
    : [
        `DROP TABLE IF EXISTS ${stageTableReference};`,
        `CREATE UNLOGGED TABLE ${stageTableReference} (LIKE public.${quotedTableName} INCLUDING DEFAULTS);`,
      ].join("\n");
  const conflictAction =
    table.updateList.length > 0 ? `DO UPDATE SET ${table.updateList}` : "DO NOTHING";
  const deduplicatedStageSelect = [
    `SELECT ${table.columnList}`,
    "FROM (",
    `  SELECT DISTINCT ON (${table.primaryKeyList}) ${table.columnList}`,
    `  FROM ${stageTableReference}`,
    `  ORDER BY ${table.primaryKeyList}`,
    ") AS stage",
  ].join("\n");
  // OVERRIDING SYSTEM VALUE preserves Neon-side GENERATED ALWAYS identity columns while remaining a no-op for non-identity columns and BY DEFAULT identities.
  const applySql = [
    `INSERT INTO public.${quotedTableName} (${table.columnList}) OVERRIDING SYSTEM VALUE ${deduplicatedStageSelect} ON CONFLICT (${table.primaryKeyList}) ${conflictAction};`,
  ].join("\n");
  const dropStageSql = temporaryStage ? "" : `DROP TABLE ${stageTableReference};\n`;
  return {
    preCopySql: [temporaryStage ? "BEGIN;" : "", stageCreateSql].join("\n"),
    copySql: `COPY ${stageTableReference} (${table.columnList}) FROM STDIN WITH (FORMAT csv, NULL '\\N');`,
    postCopySql: [temporaryStage ? "" : "BEGIN;", applySql, `${dropStageSql}COMMIT;`].join("\n"),
    cleanupSql: temporaryStage ? "ROLLBACK;" : `DROP TABLE IF EXISTS ${stageTableReference};`,
  };
}

export function quoteLiteral(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

export function buildTableFilterSql(selectedTables: string[] | undefined): string {
  if (!selectedTables || selectedTables.length === 0) {
    return "true";
  }

  return `c.relname in (${selectedTables.map(quoteLiteral).join(",")})`;
}

export function buildMetadataSql(selectedTables: string[] | undefined): string {
  const tableFilterSql = buildTableFilterSql(selectedTables);

  return `
with pk_cols as (
  select
    i.indrelid,
    array_agg(a.attname order by x.ord) as pk_names,
    string_agg(format('%I', a.attname), ', ' order by x.ord) as pk_list,
    string_agg(format('target.%1$I = stage.%1$I', a.attname), ' and ' order by x.ord) as pk_join
  from pg_index i
  join lateral unnest(i.indkey) with ordinality as x(attnum, ord) on true
  join pg_attribute a on a.attrelid = i.indrelid and a.attnum = x.attnum
  where i.indisprimary
  group by i.indrelid
),
cols as (
  select
    c.oid,
    c.relname,
    greatest(c.reltuples::bigint, 0) as est_rows,
    pg_total_relation_size(c.oid) as est_bytes,
    a.attnum,
    a.attname,
    pk.pk_names,
    pk.pk_list,
    pk.pk_join
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  join pg_attribute a on a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
  join pk_cols pk on pk.indrelid = c.oid
  where n.nspname = 'public'
    and c.relkind = 'r'
    and ${tableFilterSql}
)
select
  relname,
  max(est_rows) as est_rows,
  max(est_bytes) as est_bytes,
  string_agg(format('%I', attname), ', ' order by attnum) as column_list,
  max(pk_list) as pk_list,
  max(pk_join) as pk_join,
  coalesce(
    string_agg(format('%1$I = excluded.%1$I', attname), ', ' order by attnum)
      filter (where not attname = any(pk_names)),
    ''
  ) as update_list
from cols
group by oid, relname
order by relname;
`.trim();
}

export function buildDependencySql(selectedTables: string[] | undefined): string {
  const tableFilterSql = buildTableFilterSql(selectedTables);
  const childFilterSql = tableFilterSql.replaceAll("c.relname", "child.relname");
  const parentFilterSql = tableFilterSql.replaceAll("c.relname", "parent.relname");

  return `
select
  child.relname as child_table,
  parent.relname as parent_table
from pg_constraint constraint_info
join pg_class child on child.oid = constraint_info.conrelid
join pg_namespace child_namespace on child_namespace.oid = child.relnamespace
join pg_class parent on parent.oid = constraint_info.confrelid
join pg_namespace parent_namespace on parent_namespace.oid = parent.relnamespace
where constraint_info.contype = 'f'
  and child_namespace.nspname = 'public'
  and parent_namespace.nspname = 'public'
  and ${childFilterSql}
  and ${parentFilterSql}
order by parent.relname, child.relname;
`.trim();
}

export function parseTableMetadata(output: string): TableMetadata[] {
  return output
    .split(/\r?\n/)
    .map((line) => line.trimEnd())
    .filter((line) => line.length > 0)
    .map((line) => {
      const [
        tableName,
        estimatedRows,
        estimatedBytes,
        columnList,
        primaryKeyList,
        primaryKeyJoin,
        updateList = "",
      ] = line.split("\t");

      if (
        !tableName ||
        estimatedRows === undefined ||
        estimatedBytes === undefined ||
        !columnList ||
        !primaryKeyList ||
        !primaryKeyJoin
      ) {
        throw new Error(`Invalid table metadata row: ${line}`);
      }

      return {
        tableName,
        estimatedRows: Math.max(Number(estimatedRows), 0),
        estimatedBytes: Math.max(Number(estimatedBytes), 0),
        columnList,
        primaryKeyList,
        primaryKeyJoin,
        updateList,
      };
    });
}

export function parseDependencyEdges(output: string): DependencyEdge[] {
  return output
    .split(/\r?\n/)
    .map((line) => line.trimEnd())
    .filter((line) => line.length > 0)
    .map((line) => {
      const [childTable, parentTable] = line.split("\t");
      if (!childTable || !parentTable) {
        throw new Error(`Invalid dependency row: ${line}`);
      }
      return { childTable, parentTable };
    });
}

export function buildNeonApplySql(
  table: TableMetadata,
  deleteMissingRows: boolean,
  stageTableName = "replica_sync_stage",
  temporaryStage = true,
  applyMode: "replace" | "upsert" = "replace",
): {
  preCopySql: string;
  copySql: string;
  postCopySql: string;
  cleanupSql: string;
} {
  const quotedTableName = quoteIdentifier(table.tableName);
  const quotedStageTableName = quoteIdentifier(stageTableName);
  const quotedStageIndexName = quoteIdentifier(`${stageTableName}_pk`);
  const stageTableReference = temporaryStage
    ? quotedStageTableName
    : `public.${quotedStageTableName}`;
  const stageCreateSql = temporaryStage
    ? `CREATE TEMP TABLE ${quotedStageTableName} (LIKE public.${quotedTableName} INCLUDING DEFAULTS) ON COMMIT DROP;`
    : [
        `DROP TABLE IF EXISTS ${stageTableReference};`,
        `CREATE UNLOGGED TABLE ${stageTableReference} (LIKE public.${quotedTableName} INCLUDING DEFAULTS);`,
      ].join("\n");
  const cleanupSql = temporaryStage ? "ROLLBACK;" : `DROP TABLE IF EXISTS ${stageTableReference};`;
  const conflictAction =
    table.updateList.length > 0 ? `DO UPDATE SET ${table.updateList}` : "DO NOTHING";
  const deduplicatedStageSelect = [
    `SELECT ${table.columnList}`,
    "FROM (",
    `  SELECT DISTINCT ON (${table.primaryKeyList}) ${table.columnList}`,
    `  FROM ${stageTableReference}`,
    `  ORDER BY ${table.primaryKeyList}`,
    ") AS stage",
  ].join("\n");

  const deleteSql = deleteMissingRows
    ? `DELETE FROM public.${quotedTableName} AS target WHERE NOT EXISTS (SELECT 1 FROM ${stageTableReference} AS stage WHERE ${table.primaryKeyJoin});\n`
    : "";
  const dropStageSql = temporaryStage ? "" : `DROP TABLE ${stageTableReference};\n`;
  // OVERRIDING SYSTEM VALUE preserves Neon-side GENERATED ALWAYS identity columns while remaining a no-op for non-identity columns and BY DEFAULT identities.
  const applySql =
    applyMode === "replace"
      ? [
          `TRUNCATE TABLE public.${quotedTableName};`,
          `INSERT INTO public.${quotedTableName} (${table.columnList}) OVERRIDING SYSTEM VALUE ${deduplicatedStageSelect};`,
        ].join("\n")
      : [
          `CREATE INDEX ${quotedStageIndexName} ON ${stageTableReference} (${table.primaryKeyList});`,
          `INSERT INTO public.${quotedTableName} (${table.columnList}) OVERRIDING SYSTEM VALUE ${deduplicatedStageSelect} ON CONFLICT (${table.primaryKeyList}) ${conflictAction};`,
          deleteSql,
        ].join("\n");

  return {
    preCopySql: [temporaryStage ? "BEGIN;" : "", stageCreateSql].join("\n"),
    copySql: `COPY ${stageTableReference} (${table.columnList}) FROM STDIN WITH (FORMAT csv, NULL '\\N');`,
    postCopySql: [temporaryStage ? "" : "BEGIN;", applySql, `${dropStageSql}COMMIT;`].join("\n"),
    cleanupSql,
  };
}

export function calculateEtaSeconds(
  syncedEstimatedRows: number,
  totalEstimatedRows: number,
  elapsedSeconds: number,
): number {
  if (syncedEstimatedRows <= 0 || elapsedSeconds <= 0) {
    return 0;
  }

  const remainingEstimatedRows = Math.max(totalEstimatedRows - syncedEstimatedRows, 0);
  return Math.round((remainingEstimatedRows * elapsedSeconds) / syncedEstimatedRows);
}

export function buildDependencyPlan(
  tables: TableMetadata[],
  dependencyEdges: DependencyEdge[],
): TableMetadata[][] {
  const tableByName = new Map(tables.map((table) => [table.tableName, table]));
  const indegree = new Map(tables.map((table) => [table.tableName, 0]));
  const childrenByParent = new Map<string, Set<string>>();
  // indegree is keyed by every table.tableName, so a lookup with a name that
  // already passed `tableByName.has(...)` is always defined. The `?? 0` fallback
  // below is kept for TS narrowing only and is structurally unreachable.
  const readIndegree = (name: string): number => indegree.get(name) ?? 0;

  for (const edge of dependencyEdges) {
    if (!tableByName.has(edge.childTable) || !tableByName.has(edge.parentTable)) {
      continue;
    }
    if (edge.childTable === edge.parentTable) {
      continue;
    }

    const children = childrenByParent.get(edge.parentTable) ?? new Set<string>();
    if (!children.has(edge.childTable)) {
      children.add(edge.childTable);
      childrenByParent.set(edge.parentTable, children);
      indegree.set(edge.childTable, readIndegree(edge.childTable) + 1);
    }
  }

  const sortTables = (levelTables: TableMetadata[]) =>
    levelTables.sort(
      (left, right) =>
        right.estimatedBytes - left.estimatedBytes ||
        right.estimatedRows - left.estimatedRows ||
        left.tableName.localeCompare(right.tableName),
    );

  let currentLevel = sortTables(tables.filter((table) => readIndegree(table.tableName) === 0));
  const plan: TableMetadata[][] = [];
  let processedTables = 0;

  while (currentLevel.length > 0) {
    plan.push(currentLevel);
    processedTables += currentLevel.length;
    const nextNames = new Set<string>();

    for (const table of currentLevel) {
      for (const child of childrenByParent.get(table.tableName) ?? []) {
        const nextIndegree = readIndegree(child) - 1;
        indegree.set(child, nextIndegree);
        if (nextIndegree === 0) {
          nextNames.add(child);
        }
      }
    }

    currentLevel = sortTables(
      Array.from(nextNames, (tableName) => tableByName.get(tableName)).filter(
        (table): table is TableMetadata => table !== undefined,
      ),
    );
  }

  if (processedTables !== tables.length) {
    const cyclicTables = tables
      .filter((table) => readIndegree(table.tableName) > 0)
      .map((table) => table.tableName)
      .sort();
    throw new Error(`Circular table dependencies detected: ${cyclicTables.join(", ")}`);
  }

  return plan;
}

export function resolveConcurrency(tables: TableMetadata[], concurrency: SyncConcurrency): number {
  if (tables.length === 0) {
    return 0;
  }

  if (concurrency !== "auto") {
    return Math.max(1, Math.min(concurrency, tables.length));
  }

  const totalEstimatedRows = tables.reduce((sum, table) => sum + table.estimatedRows, 0);
  const maxEstimatedRows = Math.max(...tables.map((table) => table.estimatedRows));

  if (maxEstimatedRows >= 2_000_000) {
    return Math.min(2, tables.length);
  }
  if (totalEstimatedRows >= 1_000_000) {
    return Math.min(3, tables.length);
  }
  if (totalEstimatedRows >= 100_000) {
    return Math.min(4, tables.length);
  }

  return Math.min(8, tables.length);
}

export async function waitForNeonReady(
  config: PushSyncConfig,
  dependencies: Pick<PushSyncDependencies, "nowSeconds" | "sleep" | "checkNeonReady" | "report">,
): Promise<void> {
  const startedAt = dependencies.nowSeconds();
  dependencies.report({
    type: "neon-wait-start",
    timeoutSeconds: config.neonConnectTimeoutSeconds,
    retrySeconds: config.neonConnectRetrySeconds,
  });

  while (true) {
    if (await dependencies.checkNeonReady()) {
      dependencies.report({
        type: "neon-ready",
        elapsedSeconds: dependencies.nowSeconds() - startedAt,
      });
      return;
    }

    const elapsedSeconds = dependencies.nowSeconds() - startedAt;
    if (elapsedSeconds >= config.neonConnectTimeoutSeconds) {
      throw new Error(`Timed out waiting for Neon after ${elapsedSeconds}s`);
    }

    dependencies.report({
      type: "neon-wait-retry",
      elapsedSeconds,
      retrySeconds: config.neonConnectRetrySeconds,
    });
    await dependencies.sleep(config.neonConnectRetrySeconds * 1000);
  }
}

export async function runPushSync(
  tables: TableMetadata[],
  config: PushSyncConfig,
  dependencies: PushSyncDependencies,
  dependencyEdges: DependencyEdge[] = [],
): Promise<void> {
  if (tables.length === 0) {
    throw new Error("No primary-key tables matched");
  }

  const totalTables = tables.length;
  const totalEstimatedRows = tables.reduce((sum, table) => sum + table.estimatedRows, 0);
  const dependencyPlan = buildDependencyPlan(tables, dependencyEdges);
  const startedAt = dependencies.nowSeconds();
  let completedTables = 0;
  let runningTables = 0;
  let syncedEstimatedRows = 0;
  const allTableNames = dependencyPlan.flat().map((table) => table.tableName);
  const completedTableNames: string[] = [];
  const runningTableNames = new Set<string>();

  const remainingTableNames = () =>
    allTableNames.filter(
      (tableName) => !completedTableNames.includes(tableName) && !runningTableNames.has(tableName),
    );

  dependencies.report({
    type: "start",
    totalTables,
    totalEstimatedRows,
    dependencyLevels: dependencyPlan.length,
    concurrency: config.concurrency,
  });
  await waitForNeonReady(config, dependencies);

  async function runLevel(levelTables: TableMetadata[], dependencyLevel: number): Promise<void> {
    let nextIndex = 0;
    const levelConcurrency = resolveConcurrency(levelTables, config.concurrency);

    dependencies.report({
      type: "level-start",
      dependencyLevel,
      levelTables: levelTables.length,
      levelEstimatedRows: levelTables.reduce((sum, table) => sum + table.estimatedRows, 0),
      concurrency: levelConcurrency,
    });

    async function runWorker(): Promise<void> {
      while (true) {
        const table = levelTables[nextIndex];
        if (!table) {
          return;
        }
        nextIndex += 1;
        runningTables += 1;
        runningTableNames.add(table.tableName);

        const tableStartedAt = dependencies.nowSeconds();
        const elapsedSeconds = tableStartedAt - startedAt;
        dependencies.report({
          type: "table-start",
          tableName: table.tableName,
          estimatedRows: table.estimatedRows,
          dependencyLevel,
          levelConcurrency,
          runningTables,
          runningTableNames: Array.from(runningTableNames),
          completedTables,
          completedTableNames: [...completedTableNames],
          remainingTables: totalTables - completedTables - runningTables,
          remainingTableNames: remainingTableNames(),
          syncedEstimatedRows,
          remainingEstimatedRows: Math.max(totalEstimatedRows - syncedEstimatedRows, 0),
          elapsedSeconds,
          etaSeconds: calculateEtaSeconds(syncedEstimatedRows, totalEstimatedRows, elapsedSeconds),
        });

        await dependencies.syncTable(table);

        runningTables -= 1;
        runningTableNames.delete(table.tableName);
        completedTables += 1;
        completedTableNames.push(table.tableName);
        syncedEstimatedRows += table.estimatedRows;
        const doneAt = dependencies.nowSeconds();
        const doneElapsedSeconds = doneAt - startedAt;

        dependencies.report({
          type: "table-done",
          tableName: table.tableName,
          estimatedRows: table.estimatedRows,
          dependencyLevel,
          levelConcurrency,
          tableElapsedSeconds: doneAt - tableStartedAt,
          runningTables,
          runningTableNames: Array.from(runningTableNames),
          completedTables,
          completedTableNames: [...completedTableNames],
          totalTables,
          syncedEstimatedRows,
          totalEstimatedRows,
          remainingTables: totalTables - completedTables,
          remainingTableNames: remainingTableNames(),
          remainingEstimatedRows: Math.max(totalEstimatedRows - syncedEstimatedRows, 0),
          elapsedSeconds: doneElapsedSeconds,
          etaSeconds: calculateEtaSeconds(
            syncedEstimatedRows,
            totalEstimatedRows,
            doneElapsedSeconds,
          ),
        });
      }
    }

    await Promise.all(Array.from({ length: levelConcurrency }, () => runWorker()));
  }

  for (const [dependencyLevel, levelTables] of dependencyPlan.entries()) {
    await runLevel(levelTables, dependencyLevel);
  }

  dependencies.report({
    type: "complete",
    totalTables,
    totalEstimatedRows,
    elapsedSeconds: dependencies.nowSeconds() - startedAt,
  });
}

export const LOCAL_CONTAINER_NAME = "horse-racing-local-postgresql";
export const DEFAULT_NEON_PSQL_CONTAINER = LOCAL_CONTAINER_NAME;

export type NeonPsqlArgsInput = {
  neonUrl: string | undefined;
  containerName: string | undefined;
  extraArgs?: readonly string[];
};

export function buildNeonPsqlArgs(input: NeonPsqlArgsInput): string[] {
  if (!input.neonUrl) {
    throw new Error("NEON_DIRECT_DATABASE_URL is required");
  }
  const container =
    input.containerName !== undefined && input.containerName !== ""
      ? input.containerName
      : DEFAULT_NEON_PSQL_CONTAINER;
  return ["exec", "-i", container, "psql", input.neonUrl, ...(input.extraArgs ?? [])];
}

export function resolvePositiveIntegerEnv(
  override: number | null,
  raw: string | undefined,
  defaultValue: number,
): number {
  if (override !== null) return override;
  if (raw === undefined || raw === "") return defaultValue;
  const parsed = Number(raw);
  return Number.isInteger(parsed) && parsed >= 1 ? parsed : defaultValue;
}

export function resolveNonNegativeSecondsEnv(
  raw: string | undefined,
  defaultSeconds: number,
): number {
  const fallback = defaultSeconds * 1000;
  if (raw === undefined || raw === "") return fallback;
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed * 1000 : fallback;
}

export type RetryAttemptInfo = {
  attempt: number;
  maxAttempts: number;
};

export type RetryFailureInfo = RetryAttemptInfo & {
  error: unknown;
  retryDelayMs: number;
};

export type RetryGaveUpInfo = RetryAttemptInfo & {
  error: unknown;
};

export type RetryOptions = {
  maxAttempts: number;
  retryDelayMs: number;
  sleep: (milliseconds: number) => Promise<void>;
  computeDelayMs?: (attempt: number) => number;
  onAttemptFailed?: (info: RetryFailureInfo) => void;
  onGaveUp?: (info: RetryGaveUpInfo) => void;
  onRetrySucceeded?: (info: RetryAttemptInfo) => void;
};

export async function runWithRetry<T>(
  operation: () => Promise<T>,
  options: RetryOptions,
): Promise<T> {
  return runRetryAttempt(operation, options, 1);
}

async function runRetryAttempt<T>(
  operation: () => Promise<T>,
  options: RetryOptions,
  attempt: number,
): Promise<T> {
  try {
    const result = await operation();
    if (attempt > 1) {
      options.onRetrySucceeded?.({ attempt, maxAttempts: options.maxAttempts });
    }
    return result;
  } catch (error) {
    if (attempt >= options.maxAttempts) {
      options.onGaveUp?.({ attempt, maxAttempts: options.maxAttempts, error });
      throw error;
    }
    const delayMs = options.computeDelayMs ? options.computeDelayMs(attempt) : options.retryDelayMs;
    options.onAttemptFailed?.({
      attempt,
      maxAttempts: options.maxAttempts,
      error,
      retryDelayMs: delayMs,
    });
    await options.sleep(delayMs);
    return runRetryAttempt(operation, options, attempt + 1);
  }
}

export interface RetryBackoffConfig {
  baseMs: number;
  maxMs: number;
  jitterMs: number;
  random?: () => number;
}

const DEFAULT_RETRY_BASE_SECONDS = 5;
const DEFAULT_RETRY_MAX_SECONDS = 60;
const DEFAULT_RETRY_JITTER_MS = 1000;

export function computeBackoffDelayMs(attempt: number, config: RetryBackoffConfig): number {
  const safeAttempt = Math.max(1, Math.floor(attempt));
  const exponent = safeAttempt - 1;
  const exponentialMs = config.baseMs * 2 ** exponent;
  const randomFn = config.random ?? Math.random;
  const jitter = config.jitterMs > 0 ? randomFn() * config.jitterMs : 0;
  const total = exponentialMs + jitter;
  return Math.min(total, config.maxMs);
}

export function resolveRetryBackoffConfig(
  env: Record<string, string | undefined>,
): RetryBackoffConfig {
  const legacyBaseSeconds = parseFiniteNonNegativeNumber(env.REPLICA_SYNC_RETRY_DELAY_SECONDS);
  const baseSeconds =
    parseFiniteNonNegativeNumber(env.REPLICA_SYNC_RETRY_BASE_DELAY_SECONDS) ??
    legacyBaseSeconds ??
    DEFAULT_RETRY_BASE_SECONDS;
  const maxSeconds =
    parseFiniteNonNegativeNumber(env.REPLICA_SYNC_RETRY_MAX_DELAY_SECONDS) ??
    DEFAULT_RETRY_MAX_SECONDS;
  const baseMs = baseSeconds * 1000;
  const maxMs = Math.max(baseMs, maxSeconds * 1000);
  return {
    baseMs,
    maxMs,
    jitterMs: DEFAULT_RETRY_JITTER_MS,
  };
}

function parseFiniteNonNegativeNumber(raw: string | undefined): number | null {
  if (raw === undefined || raw === "") return null;
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
}

export interface VerifyMismatchPolicy {
  thresholdRows: number;
  largeTableRows: number;
  forceFullReplace: boolean;
  reincrementalMaxDiffPercent: number;
}

export interface VerifyMismatchInput {
  tableName: string;
  localCount: number;
  neonCount: number;
  rowCount: number;
  policy: VerifyMismatchPolicy;
}

export type VerifyMismatchAction =
  | { kind: "fallback-full"; message: string }
  | { kind: "skip"; message: string; reason: string }
  | { kind: "re-incremental"; message: string; reason: string; diffPercent: number };

const DEFAULT_VERIFY_MISMATCH_THRESHOLD_ROWS = 10;
const DEFAULT_VERIFY_MISMATCH_LARGE_TABLE_ROWS = 100_000;
const DEFAULT_VERIFY_MISMATCH_REINCREMENTAL_PERCENT = 1;
const PERCENT_DIVISOR = 100;

export function decideVerifyMismatchAction(input: VerifyMismatchInput): VerifyMismatchAction {
  const diff = Math.abs(input.localCount - input.neonCount);
  const isLargeTable = input.rowCount >= input.policy.largeTableRows;
  const isSmallDiff = diff <= input.policy.thresholdRows;
  const shouldSkip = !input.policy.forceFullReplace && isLargeTable && isSmallDiff;
  if (shouldSkip) {
    const reason = `verify mismatch (local=${input.localCount}, neon=${input.neonCount}, diff=${diff})`;
    return {
      kind: "skip",
      reason,
      message: `${input.tableName}: ${reason} — full-replace too costly (${input.rowCount} rows, ≥${input.policy.largeTableRows} threshold). Skipping. Run reconcile or set REPLICA_VERIFY_MISMATCH_FORCE_FULL_REPLACE=true.`,
    };
  }
  const denominator = Math.max(input.localCount, input.neonCount, 1);
  const diffPercent = (diff / denominator) * PERCENT_DIVISOR;
  const shouldReincremental =
    !input.policy.forceFullReplace &&
    isLargeTable &&
    diffPercent < input.policy.reincrementalMaxDiffPercent;
  if (shouldReincremental) {
    const reason = `verify mismatch (local=${input.localCount}, neon=${input.neonCount}, diff=${diff}, ${diffPercent.toFixed(3)}%)`;
    return {
      kind: "re-incremental",
      reason,
      diffPercent,
      message: `${input.tableName}: ${reason} — under ${input.policy.reincrementalMaxDiffPercent}% drift, retrying as re-incremental instead of full-replace`,
    };
  }
  return {
    kind: "fallback-full",
    message: `${input.tableName}: verify mismatch (local=${input.localCount}, neon=${input.neonCount}, diff=${diff}) — falling back to full-replace`,
  };
}

export function resolveVerifyMismatchPolicy(
  env: Record<string, string | undefined>,
): VerifyMismatchPolicy {
  return {
    thresholdRows: parsePositiveIntegerOrZero(
      env.REPLICA_VERIFY_MISMATCH_THRESHOLD_ROWS,
      DEFAULT_VERIFY_MISMATCH_THRESHOLD_ROWS,
    ),
    largeTableRows: parsePositiveIntegerOrZero(
      env.REPLICA_VERIFY_MISMATCH_LARGE_TABLE_ROWS,
      DEFAULT_VERIFY_MISMATCH_LARGE_TABLE_ROWS,
    ),
    forceFullReplace: parseBoolean(env.REPLICA_VERIFY_MISMATCH_FORCE_FULL_REPLACE, false),
    reincrementalMaxDiffPercent: parseFiniteNonNegativeNumberOrFallback(
      env.REPLICA_SYNC_FULL_REPLACE_THRESHOLD_PERCENT,
      DEFAULT_VERIFY_MISMATCH_REINCREMENTAL_PERCENT,
    ),
  };
}

function parseFiniteNonNegativeNumberOrFallback(raw: string | undefined, fallback: number): number {
  const parsed = parseFiniteNonNegativeNumber(raw);
  return parsed === null ? fallback : parsed;
}

function parsePositiveIntegerOrZero(raw: string | undefined, fallback: number): number {
  if (raw === undefined || raw === "") return fallback;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < 0) return fallback;
  return parsed;
}

export interface VerifyMismatchSkipErrorInit {
  tableName: string;
  localCount: number;
  neonCount: number;
  rowCount: number;
  message: string;
}

export class VerifyMismatchSkipError extends Error {
  readonly tableName: string;
  readonly localCount: number;
  readonly neonCount: number;
  readonly rowCount: number;

  constructor(init: VerifyMismatchSkipErrorInit) {
    super(init.message);
    this.name = "VerifyMismatchSkipError";
    this.tableName = init.tableName;
    this.localCount = init.localCount;
    this.neonCount = init.neonCount;
    this.rowCount = init.rowCount;
  }
}

export function isVerifyMismatchSkipError(error: unknown): error is VerifyMismatchSkipError {
  return error instanceof VerifyMismatchSkipError;
}

const DEFAULT_FULL_REPLACE_COPY_BATCH_ROWS = 500_000;
const DEFAULT_OPERATION_WALL_CLOCK_TIMEOUT_SECONDS = 3600;
// Raised from 300s to 900s because postCopySql (CREATE INDEX / PRIMARY KEY) for
// large tables (e.g. race_finish_position_model_predictions ~10M rows) can take
// 5-15 minutes on Neon while emitting no output. Wall-clock timeout still caps
// the whole operation at 1h, so the bigger idle window does not extend total runtime.
const DEFAULT_OPERATION_IDLE_TIMEOUT_SECONDS = 900;
const TIMEOUT_WARNING_RATIO = 0.8;
const PER_TABLE_TIMEOUT_ENV_PREFIX = "REPLICA_SYNC_OPERATION_TIMEOUT_SECONDS_";
const PER_TABLE_IDLE_TIMEOUT_ENV_PREFIX = "REPLICA_SYNC_IDLE_TIMEOUT_SECONDS_";
const PER_TABLE_SKIP_ENV = "REPLICA_SYNC_SKIP_TABLES";

export function resolveSkipTables(env: Record<string, string | undefined>): ReadonlySet<string> {
  const raw = env[PER_TABLE_SKIP_ENV]?.trim();
  if (!raw) return new Set();
  return new Set(
    raw
      .split(",")
      .map((entry) => entry.trim())
      .filter((entry) => entry.length > 0),
  );
}

export function resolveDefaultFullReplaceBatchRows(
  env: Record<string, string | undefined>,
): number {
  const raw = env.REPLICA_SYNC_COPY_BATCH_ROWS;
  if (raw === undefined || raw === "") return DEFAULT_FULL_REPLACE_COPY_BATCH_ROWS;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) return DEFAULT_FULL_REPLACE_COPY_BATCH_ROWS;
  return parsed;
}

export interface ChunkPlan {
  chunkRows: number;
  chunkCount: number;
}

export function computeChunkPlan(rowCount: number, batchRows: number): ChunkPlan {
  if (rowCount <= 0) return { chunkRows: batchRows, chunkCount: 0 };
  if (batchRows <= 0) return { chunkRows: rowCount, chunkCount: 1 };
  return { chunkRows: batchRows, chunkCount: Math.ceil(rowCount / batchRows) };
}

export interface OperationTimeoutPolicy {
  wallClockMs: number;
  idleMs: number;
  warningRatio: number;
}

export function resolveOperationTimeoutPolicy(
  env: Record<string, string | undefined>,
): OperationTimeoutPolicy {
  const wallClockSeconds = parsePositiveIntegerOrZero(
    env.REPLICA_SYNC_OPERATION_TIMEOUT_SECONDS,
    DEFAULT_OPERATION_WALL_CLOCK_TIMEOUT_SECONDS,
  );
  const idleSeconds = parsePositiveIntegerOrZero(
    env.REPLICA_SYNC_IDLE_TIMEOUT_SECONDS,
    DEFAULT_OPERATION_IDLE_TIMEOUT_SECONDS,
  );
  return {
    wallClockMs: Math.max(wallClockSeconds, 1) * 1000,
    idleMs: Math.max(idleSeconds, 1) * 1000,
    warningRatio: TIMEOUT_WARNING_RATIO,
  };
}

export interface PerTableTimeoutLookupInput {
  env: Record<string, string | undefined>;
  tableName: string;
  fallbackWallClockMs: number;
}

export interface PerTableIdleTimeoutLookupInput {
  env: Record<string, string | undefined>;
  tableName: string;
  fallbackIdleMs: number;
}

export function resolvePerTableWallClockMs(input: PerTableTimeoutLookupInput): number {
  const raw = input.env[`${PER_TABLE_TIMEOUT_ENV_PREFIX}${input.tableName}`];
  if (raw === undefined || raw === "") return input.fallbackWallClockMs;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) return input.fallbackWallClockMs;
  return parsed * 1000;
}

export function resolvePerTableIdleMs(input: PerTableIdleTimeoutLookupInput): number {
  const raw = input.env[`${PER_TABLE_IDLE_TIMEOUT_ENV_PREFIX}${input.tableName}`];
  if (raw === undefined || raw === "") return input.fallbackIdleMs;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) return input.fallbackIdleMs;
  return Math.max(parsed, 1) * 1000;
}

export function formatRowsPerSecond(rowsDone: number, elapsedSeconds: number): number {
  if (elapsedSeconds <= 0 || rowsDone <= 0) return 0;
  return rowsDone / elapsedSeconds;
}

export function computeChunkEtaSeconds(
  rowsDone: number,
  rowsTotal: number,
  elapsedSeconds: number,
): number {
  if (rowsDone <= 0 || elapsedSeconds <= 0) return 0;
  const rowsRemaining = Math.max(rowsTotal - rowsDone, 0);
  return Math.round((rowsRemaining * elapsedSeconds) / rowsDone);
}

export interface JsonlRecordInput {
  tsIso: string;
  event: ProgressEvent;
  elapsedSeconds: number;
  attempt?: number;
  attemptMax?: number;
}

export interface JsonlRecord {
  ts: string;
  event: ProgressEvent["type"];
  table?: string;
  chunk_index?: number;
  chunk_count?: number;
  rows?: number;
  rows_total?: number;
  rows_per_second?: number;
  elapsed_s: number;
  eta_table_s?: number;
  eta_total_s?: number;
  attempt?: number;
  attempt_max?: number;
}

export function buildJsonlRecord(input: JsonlRecordInput): JsonlRecord {
  const base: JsonlRecord = {
    ts: input.tsIso,
    event: input.event.type,
    elapsed_s: input.elapsedSeconds,
  };
  const enriched = mergeRecordFields(base, input.event);
  return mergeOptionalAttempts(enriched, input.attempt, input.attemptMax);
}

function mergeRecordFields(base: JsonlRecord, event: ProgressEvent): JsonlRecord {
  if (event.type === "chunk-plan") {
    return {
      ...base,
      table: event.tableName,
      chunk_count: event.chunkCount,
      rows: event.chunkRows,
      rows_total: event.rowCount,
    };
  }
  if (event.type === "chunk-done") {
    return {
      ...base,
      table: event.tableName,
      chunk_index: event.chunkIndex,
      chunk_count: event.chunkCount,
      rows: event.rowsDone,
      rows_total: event.rowsTotal,
      rows_per_second: event.rowsPerSecond,
      eta_table_s: event.etaTableSeconds,
    };
  }
  if (event.type === "table-start") {
    return {
      ...base,
      table: event.tableName,
      rows_total: event.estimatedRows,
      eta_total_s: event.etaSeconds,
    };
  }
  if (event.type === "table-done") {
    return {
      ...base,
      table: event.tableName,
      rows_total: event.estimatedRows,
      eta_total_s: event.etaSeconds,
    };
  }
  if (event.type === "timeout-warning") {
    return { ...base, table: event.tableName };
  }
  return base;
}

function mergeOptionalAttempts(
  record: JsonlRecord,
  attempt: number | undefined,
  attemptMax: number | undefined,
): JsonlRecord {
  if (attempt === undefined && attemptMax === undefined) return record;
  return { ...record, attempt, attempt_max: attemptMax };
}
