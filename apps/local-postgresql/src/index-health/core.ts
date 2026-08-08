/**
 * Local PostgreSQL index health: detect B-tree corruption (XX002 / amcheck)
 * and repair via REINDEX (never DROP INDEX). Also dedupe unique-key heap rows
 * that slipped in while a unique index was corrupted.
 */

export type IndexKind = "unique" | "nonunique";

export type IndexTarget = {
  schemaName: string;
  tableName: string;
  indexName: string;
  kind: IndexKind;
  keyColumns: string[];
};

export type AmcheckFailure = {
  indexName: string;
  tableName: string;
  message: string;
};

export type DuplicateGroup = {
  tableName: string;
  indexName: string;
  keyColumns: string[];
  duplicateRowCount: number;
};

export type RepairAction =
  | {
      type: "dedupe_unique_heap";
      tableName: string;
      indexName: string;
      keyColumns: string[];
      preferNewestByColumn: string | null;
    }
  | {
      type: "reindex_index";
      indexName: string;
      tableName: string;
    }
  | {
      type: "reindex_system_catalogs";
    }
  | {
      type: "drop_orphan_temp_relation";
      oid: number;
      relname: string;
    };

export type HealthReport = {
  checkedIndexCount: number;
  amcheckFailures: AmcheckFailure[];
  duplicateGroups: DuplicateGroup[];
  orphanTempRelations: Array<{ oid: number; relname: string; relnatts: number }>;
  systemCatalogFailures: AmcheckFailure[];
  repairActions: RepairAction[];
  healthy: boolean;
};

export type IndexHealthConfig = {
  /** Schemas whose btree indexes are scanned. */
  schemas: string[];
  /** Table name prefixes that receive priority scan + repair (PC-KEIBA ingest). */
  priorityTablePrefixes: string[];
  /** Timestamp-like column used to keep the newest row when deduping. */
  dedupePreferNewestColumns: string[];
  /** System catalog indexes to amcheck (superuser). */
  systemCatalogIndexes: string[];
};

export const DEFAULT_INDEX_HEALTH_CONFIG: IndexHealthConfig = {
  schemas: ["public"],
  priorityTablePrefixes: ["nvd_", "jvd_"],
  dedupePreferNewestColumns: ["data_sakusei_nengappi", "updated_at", "update_timestamp"],
  systemCatalogIndexes: [
    "pg_class_oid_index",
    "pg_class_relname_nsp_index",
    "pg_class_tblspc_relfilenode_index",
  ],
};

export function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

export function quoteLiteral(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

export function isPriorityTable(
  tableName: string,
  prefixes: readonly string[] = DEFAULT_INDEX_HEALTH_CONFIG.priorityTablePrefixes,
): boolean {
  return prefixes.some((prefix) => tableName.startsWith(prefix));
}

export function buildEnsureAmcheckExtensionSql(): string {
  return "CREATE EXTENSION IF NOT EXISTS amcheck;";
}

export function buildListBtreeIndexesSql(
  config: IndexHealthConfig = DEFAULT_INDEX_HEALTH_CONFIG,
): string {
  const schemaList = config.schemas.map((s) => quoteLiteral(s)).join(", ");
  return `
SELECT
  n.nspname AS schema_name,
  t.relname AS table_name,
  i.relname AS index_name,
  CASE WHEN ix.indisunique THEN 'unique' ELSE 'nonunique' END AS kind,
  COALESCE(
    (
      SELECT array_agg(a.attname::text ORDER BY x.ordinality)
      FROM unnest(ix.indkey) WITH ORDINALITY AS x(attnum, ordinality)
      JOIN pg_attribute a
        ON a.attrelid = t.oid AND a.attnum = x.attnum
      WHERE x.attnum > 0
    ),
    ARRAY[]::text[]
  ) AS key_columns
FROM pg_index ix
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN pg_class t ON t.oid = ix.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_am am ON am.oid = i.relam
WHERE n.nspname IN (${schemaList})
  AND i.relkind = 'i'
  AND am.amname = 'btree'
  AND NOT ix.indisexclusion
ORDER BY t.relname, i.relname;
`.trim();
}

export function buildAmcheckCallSql(indexName: string, heapallindexed = true): string {
  return `SELECT bt_index_check(${quoteLiteral(indexName)}::regclass, ${heapallindexed ? "true" : "false"});`;
}

export function buildSystemCatalogAmcheckSql(
  indexName: string,
  config: IndexHealthConfig = DEFAULT_INDEX_HEALTH_CONFIG,
): string | null {
  if (!config.systemCatalogIndexes.includes(indexName)) {
    return null;
  }
  return `SELECT bt_index_check(${quoteLiteral(indexName)}::regclass, true);`;
}

export function buildDuplicateCountSql(target: IndexTarget): string | null {
  if (target.kind !== "unique" || target.keyColumns.length === 0) {
    return null;
  }
  const cols = target.keyColumns.map(quoteIdentifier).join(", ");
  return `
SELECT count(*)::bigint AS duplicate_row_count
FROM (
  SELECT 1
  FROM ${quoteIdentifier(target.schemaName)}.${quoteIdentifier(target.tableName)}
  GROUP BY ${cols}
  HAVING count(*) > 1
) d;
`.trim();
}

export function resolveDedupePreferColumn(
  availableColumns: readonly string[],
  preferColumns: readonly string[] = DEFAULT_INDEX_HEALTH_CONFIG.dedupePreferNewestColumns,
): string | null {
  for (const candidate of preferColumns) {
    if (availableColumns.includes(candidate)) {
      return candidate;
    }
  }
  return null;
}

export function buildDedupeUniqueHeapSql(
  target: IndexTarget,
  preferNewestByColumn: string | null,
): string | null {
  if (target.kind !== "unique" || target.keyColumns.length === 0) {
    return null;
  }
  const cols = target.keyColumns.map(quoteIdentifier).join(", ");
  const orderBy =
    preferNewestByColumn === null
      ? "ctid DESC"
      : `${quoteIdentifier(preferNewestByColumn)} DESC NULLS LAST, ctid DESC`;
  return `
WITH ranked AS (
  SELECT ctid AS row_ctid,
         ROW_NUMBER() OVER (
           PARTITION BY ${cols}
           ORDER BY ${orderBy}
         ) AS rn
  FROM ${quoteIdentifier(target.schemaName)}.${quoteIdentifier(target.tableName)}
),
deleted AS (
  DELETE FROM ${quoteIdentifier(target.schemaName)}.${quoteIdentifier(target.tableName)} AS t
  USING ranked r
  WHERE t.ctid = r.row_ctid
    AND r.rn > 1
  RETURNING 1
)
SELECT count(*)::bigint AS deleted_rows FROM deleted;
`.trim();
}

/** REINDEX INDEX rebuilds the index in place. Never DROP INDEX. */
export function buildReindexIndexSql(indexName: string): string {
  return `REINDEX INDEX ${quoteIdentifier(indexName)};`;
}

export function buildReindexSystemCatalogsSql(
  config: IndexHealthConfig = DEFAULT_INDEX_HEALTH_CONFIG,
): string[] {
  return config.systemCatalogIndexes.map((name) => buildReindexIndexSql(name));
}

export function buildListOrphanTempRelationsSql(): string {
  return `
SELECT c.oid,
       c.relname,
       c.relnatts,
       (
         SELECT count(*)::int
         FROM pg_attribute a
         WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
       ) AS attr_count
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname LIKE 'pg_temp_%'
  AND c.relkind = 'r'
ORDER BY c.oid;
`.trim();
}

/**
 * Catalog-only cleanup for orphan temp tables whose pg_attribute rows are gone
 * (DROP TABLE itself fails with "pg_attribute catalog is missing ...").
 * Does not touch user indexes.
 */
export function buildDropOrphanTempRelationSql(oid: number): string {
  return `
SET allow_system_table_mods = on;
BEGIN;
DELETE FROM pg_depend WHERE objid = ${oid} OR refobjid = ${oid};
DELETE FROM pg_type WHERE typrelid = ${oid};
DELETE FROM pg_class WHERE oid = ${oid};
COMMIT;
`.trim();
}

export function isOrphanTempCorrupt(relnatts: number, attrCount: number): boolean {
  return attrCount === 0 && relnatts > 0;
}

export function parseAmcheckErrorMessage(errorText: string): string {
  const trimmed = errorText.trim();
  const detail = /DETAIL:\s*(.+)$/im.exec(trimmed)?.[1]?.trim();
  if (detail) {
    return detail;
  }
  const error = /ERROR:\s*(.+)$/im.exec(trimmed)?.[1]?.trim();
  if (error) {
    return error;
  }
  return trimmed;
}

export function isIndexCorruptionMessage(message: string): boolean {
  const lower = message.toLowerCase();
  return (
    lower.includes("invariant violated") ||
    lower.includes("xx002") ||
    lower.includes("overlaps with invalid duplicate") ||
    lower.includes("lacks matching index tuple") ||
    lower.includes("points to heap-only tuple") ||
    lower.includes("could not read block") ||
    lower.includes("invalid page header") ||
    lower.includes("index row size") ||
    lower.includes("right sibling") ||
    lower.includes("left link")
  );
}

export function planRepairActions(input: {
  amcheckFailures: AmcheckFailure[];
  duplicateGroups: DuplicateGroup[];
  orphanTempRelations: Array<{ oid: number; relname: string; relnatts: number; attrCount: number }>;
  systemCatalogFailures: AmcheckFailure[];
  preferNewestByColumnByTable: Record<string, string | null>;
}): RepairAction[] {
  const actions: RepairAction[] = [];
  const reindexed = new Set<string>();

  for (const orphan of input.orphanTempRelations) {
    if (isOrphanTempCorrupt(orphan.relnatts, orphan.attrCount)) {
      actions.push({
        type: "drop_orphan_temp_relation",
        oid: orphan.oid,
        relname: orphan.relname,
      });
    }
  }

  if (input.systemCatalogFailures.length > 0) {
    actions.push({ type: "reindex_system_catalogs" });
  }

  for (const dup of input.duplicateGroups) {
    if (dup.duplicateRowCount <= 0) {
      continue;
    }
    actions.push({
      type: "dedupe_unique_heap",
      tableName: dup.tableName,
      indexName: dup.indexName,
      keyColumns: dup.keyColumns,
      preferNewestByColumn: input.preferNewestByColumnByTable[dup.tableName] ?? null,
    });
  }

  for (const failure of input.amcheckFailures) {
    if (reindexed.has(failure.indexName)) {
      continue; // same index may appear once from amcheck + once from related noise
    }
    reindexed.add(failure.indexName);
    actions.push({
      type: "reindex_index",
      indexName: failure.indexName,
      tableName: failure.tableName,
    });
  }

  return actions;
}

export function buildHealthReport(input: {
  checkedIndexCount: number;
  amcheckFailures: AmcheckFailure[];
  duplicateGroups: DuplicateGroup[];
  orphanTempRelations: Array<{ oid: number; relname: string; relnatts: number; attrCount: number }>;
  systemCatalogFailures: AmcheckFailure[];
  preferNewestByColumnByTable: Record<string, string | null>;
}): HealthReport {
  const repairActions = planRepairActions(input);
  return {
    checkedIndexCount: input.checkedIndexCount,
    amcheckFailures: input.amcheckFailures,
    duplicateGroups: input.duplicateGroups.filter((g) => g.duplicateRowCount > 0),
    orphanTempRelations: input.orphanTempRelations.map((o) => ({
      oid: o.oid,
      relname: o.relname,
      relnatts: o.relnatts,
    })),
    systemCatalogFailures: input.systemCatalogFailures,
    repairActions,
    healthy: repairActions.length === 0,
  };
}

export function summarizeHealthReport(report: HealthReport): string {
  if (report.healthy) {
    return `index-health: OK (checked ${report.checkedIndexCount} btree indexes)`;
  }
  const parts = [
    `index-health: UNHEALTHY (checked ${report.checkedIndexCount})`,
    `amcheck_failures=${report.amcheckFailures.length}`,
    `duplicate_groups=${report.duplicateGroups.length}`,
    `system_catalog_failures=${report.systemCatalogFailures.length}`,
    `orphan_temps=${report.orphanTempRelations.length}`,
    `repair_actions=${report.repairActions.length}`,
  ];
  return parts.join(" ");
}

export function orderTargetsForCheck(
  targets: IndexTarget[],
  config: IndexHealthConfig = DEFAULT_INDEX_HEALTH_CONFIG,
): IndexTarget[] {
  const priority: IndexTarget[] = [];
  const rest: IndexTarget[] = [];
  for (const target of targets) {
    if (isPriorityTable(target.tableName, config.priorityTablePrefixes)) {
      priority.push(target);
    } else {
      rest.push(target);
    }
  }
  return [...priority, ...rest];
}

export type ParsedIndexRow = {
  schema_name: string;
  table_name: string;
  index_name: string;
  kind: string;
  key_columns: string[] | string;
};

export function parseIndexTargetRow(row: ParsedIndexRow): IndexTarget {
  const keyColumns = Array.isArray(row.key_columns)
    ? row.key_columns
    : parsePgTextArray(row.key_columns);
  return {
    schemaName: row.schema_name,
    tableName: row.table_name,
    indexName: row.index_name,
    kind: row.kind === "unique" ? "unique" : "nonunique",
    keyColumns,
  };
}

/** Parse PostgreSQL text-array literal like `{a,b}` or `{"a b"}`. */
export function parsePgTextArray(raw: string): string[] {
  const trimmed = raw.trim();
  if (trimmed === "") {
    return [];
  }
  if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
    return [trimmed];
  }
  const inner = trimmed.slice(1, -1);
  const result: string[] = [];
  if (inner === "") {
    return result;
  }
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < inner.length; i += 1) {
    const ch = inner[i];
    if (ch === '"') {
      inQuotes = !inQuotes;
      continue;
    }
    if (ch === "," && !inQuotes) {
      result.push(current);
      current = "";
      continue;
    }
    current += ch;
  }
  result.push(current);
  return result;
}

export function buildTableColumnsSql(schemaName: string, tableName: string): string {
  return `
SELECT a.attname::text AS column_name
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ${quoteLiteral(schemaName)}
  AND c.relname = ${quoteLiteral(tableName)}
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;
`.trim();
}
