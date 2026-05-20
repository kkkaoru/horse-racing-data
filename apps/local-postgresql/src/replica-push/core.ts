export type TableMetadata = {
  tableName: string;
  estimatedRows: number;
  estimatedBytes: number;
  columnList: string;
  primaryKeyList: string;
  primaryKeyJoin: string;
  updateList: string;
};

export type DependencyEdge = {
  childTable: string;
  parentTable: string;
};

export type SyncConcurrency = number | "auto";

export type SyncStrategy =
  | "timestamp-incremental"
  | "pk-incremental"
  | "full-replace";

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
const TIMESTAMP_COLUMN_PRIORITY: readonly string[] = [
  "updated_at",
  "update_timestamp",
  "prediction_generated_at",
  "evaluated_at",
  "activated_at",
  "modified_at",
  "generated_at",
];

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
  const lines = output.split("\n").map((line) => line.trim()).filter((line) => line !== "");
  return lines.map((line) => parseTableProfileLine(line, thresholds, mode));
}

function parseTableProfileLine(
  line: string,
  thresholds: SyncStrategyThresholds,
  mode: "auto" | "full",
): TableProfile {
  const cells = line.split("\t");
  const tableName = cells[0] ?? "";
  const rowCount = Number(cells[1] ?? "0");
  const nTupUpd = Number(cells[2] ?? "0");
  const hasPrimaryKey = (cells[3] ?? "f") === "t";
  const tsColumn = cells[4] ?? "";
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

export function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
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
  const [countText, marker] = line.trim().split("\t");
  return {
    count: Number(countText ?? "0"),
    marker: marker ?? "",
  };
}

export function buildIncrementalCopyFromSql(
  table: TableMetadata,
  options: { keyExpression: string; neonMarker: string; comparator: ">" },
): string {
  const sanitizedMarker = options.neonMarker.replaceAll("'", "''");
  const where = sanitizedMarker === ""
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
  const applySql = [
    `INSERT INTO public.${quotedTableName} (${table.columnList}) ${deduplicatedStageSelect} ON CONFLICT (${table.primaryKeyList}) ${conflictAction};`,
  ].join("\n");
  const dropStageSql = temporaryStage ? "" : `DROP TABLE ${stageTableReference};\n`;
  return {
    preCopySql: [temporaryStage ? "BEGIN;" : "", stageCreateSql].join("\n"),
    copySql: `COPY ${stageTableReference} (${table.columnList}) FROM STDIN WITH (FORMAT csv, NULL '\\N');`,
    postCopySql: [
      temporaryStage ? "" : "BEGIN;",
      applySql,
      `${dropStageSql}COMMIT;`,
    ].join("\n"),
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
  const applySql =
    applyMode === "replace"
      ? [
          `TRUNCATE TABLE public.${quotedTableName};`,
          `INSERT INTO public.${quotedTableName} (${table.columnList}) ${deduplicatedStageSelect};`,
        ].join("\n")
      : [
          `CREATE INDEX ${quotedStageIndexName} ON ${stageTableReference} (${table.primaryKeyList});`,
          `INSERT INTO public.${quotedTableName} (${table.columnList}) ${deduplicatedStageSelect} ON CONFLICT (${table.primaryKeyList}) ${conflictAction};`,
          deleteSql,
        ].join("\n");

  return {
    preCopySql: [
      temporaryStage ? "BEGIN;" : "",
      stageCreateSql,
    ].join("\n"),
    copySql: `COPY ${stageTableReference} (${table.columnList}) FROM STDIN WITH (FORMAT csv, NULL '\\N');`,
    postCopySql: [
      temporaryStage ? "" : "BEGIN;",
      applySql,
      `${dropStageSql}COMMIT;`,
    ].join("\n"),
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
      indegree.set(edge.childTable, (indegree.get(edge.childTable) ?? 0) + 1);
    }
  }

  const sortTables = (levelTables: TableMetadata[]) =>
    levelTables.sort(
      (left, right) =>
        right.estimatedBytes - left.estimatedBytes ||
        right.estimatedRows - left.estimatedRows ||
        left.tableName.localeCompare(right.tableName),
    );

  let currentLevel = sortTables(
    tables.filter((table) => (indegree.get(table.tableName) ?? 0) === 0),
  );
  const plan: TableMetadata[][] = [];
  let processedTables = 0;

  while (currentLevel.length > 0) {
    plan.push(currentLevel);
    processedTables += currentLevel.length;
    const nextNames = new Set<string>();

    for (const table of currentLevel) {
      for (const child of childrenByParent.get(table.tableName) ?? []) {
        const nextIndegree = (indegree.get(child) ?? 0) - 1;
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
      .filter((table) => (indegree.get(table.tableName) ?? 0) > 0)
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
      (tableName) =>
        !completedTableNames.includes(tableName) && !runningTableNames.has(tableName),
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
