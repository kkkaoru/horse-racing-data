#!/usr/bin/env bun
/**
 * Detect and repair local PostgreSQL B-tree index corruption (XX002 / amcheck).
 *
 * Repair uses REINDEX INDEX (rebuild in place). DROP INDEX is never used.
 * Unique-key heap duplicates are removed before REINDEX so unique rebuild can succeed.
 *
 * Usage:
 *   bun run indexes:check          # report only (exit 1 if unhealthy)
 *   bun run indexes:repair         # detect + repair + re-verify
 *   bun run indexes:repair --quick # priority nvd_/jvd_ indexes + catalogs only
 */
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  DEFAULT_INDEX_HEALTH_CONFIG,
  buildAmcheckCallSql,
  buildDedupeUniqueHeapSql,
  buildDuplicateCountSql,
  buildDropOrphanTempRelationSql,
  buildEnsureAmcheckExtensionSql,
  buildHealthReport,
  buildListBtreeIndexesSql,
  buildListOrphanTempRelationsSql,
  buildReindexIndexSql,
  buildReindexSystemCatalogsSql,
  buildTableColumnsSql,
  isPriorityTable,
  orderTargetsForCheck,
  parseAmcheckErrorMessage,
  parseIndexTargetRow,
  resolveDedupePreferColumn,
  summarizeHealthReport,
  type AmcheckFailure,
  type DuplicateGroup,
  type HealthReport,
  type IndexTarget,
  type RepairAction,
} from "../src/index-health/core";
import { LOCAL_CONTAINER_NAME } from "../src/replica-push/core";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const appDir = resolve(scriptDir, "..");

type CliOptions = {
  repair: boolean;
  quick: boolean;
};

function parseArgs(argv: string[]): CliOptions {
  return {
    repair: argv.includes("--repair"),
    quick: argv.includes("--quick"),
  };
}

function loadEnv(): { user: string; database: string } {
  const envPath = resolve(appDir, ".env");
  const env: Record<string, string> = {};
  if (existsSync(envPath)) {
    for (const line of readFileSync(envPath, "utf8").split("\n")) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#") || !trimmed.includes("=")) {
        continue;
      }
      const idx = trimmed.indexOf("=");
      const key = trimmed.slice(0, idx);
      let value = trimmed.slice(idx + 1);
      if (
        (value.startsWith('"') && value.endsWith('"')) ||
        (value.startsWith("'") && value.endsWith("'"))
      ) {
        value = value.slice(1, -1);
      }
      env[key] = value;
    }
  }
  return {
    user: process.env.POSTGRES_USER ?? env.POSTGRES_USER ?? "horse_racing",
    database: process.env.POSTGRES_DB ?? env.POSTGRES_DB ?? "horse_racing",
  };
}

type PsqlResult = {
  ok: boolean;
  stdout: string;
  stderr: string;
  status: number | null;
};

function runPsql(sql: string, user: string, database: string): PsqlResult {
  const result = spawnSync(
    "container",
    [
      "exec",
      LOCAL_CONTAINER_NAME,
      "psql",
      "-U",
      user,
      "-d",
      database,
      "-v",
      "ON_ERROR_STOP=1",
      "-t",
      "-A",
      "-F",
      "\t",
      "-c",
      sql,
    ],
    { encoding: "utf8" },
  );
  return {
    ok: result.status === 0,
    stdout: result.stdout ?? "",
    stderr: result.stderr ?? "",
    status: result.status,
  };
}

function runPsqlTuplesOnly(sql: string, user: string, database: string): string[] {
  const result = runPsql(sql, user, database);
  if (!result.ok) {
    throw new Error(`psql failed: ${result.stderr || result.stdout}`);
  }
  return result.stdout
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
}

function ensureAmcheck(user: string, database: string): void {
  const result = runPsql(buildEnsureAmcheckExtensionSql(), user, database);
  if (!result.ok) {
    throw new Error(`CREATE EXTENSION amcheck failed: ${result.stderr}`);
  }
}

function listTargets(user: string, database: string, quick: boolean): IndexTarget[] {
  const rows = runPsqlTuplesOnly(buildListBtreeIndexesSql(), user, database);
  const targets: IndexTarget[] = [];
  for (const line of rows) {
    const parts = line.split("\t");
    if (parts.length < 5) {
      continue;
    }
    const schema_name = parts[0];
    const table_name = parts[1];
    const index_name = parts[2];
    const kind = parts[3];
    if (
      schema_name === undefined ||
      table_name === undefined ||
      index_name === undefined ||
      kind === undefined
    ) {
      continue;
    }
    const target = parseIndexTargetRow({
      schema_name,
      table_name,
      index_name,
      kind,
      key_columns: parts.slice(4).join("\t"),
    });
    if (quick && !isPriorityTable(target.tableName)) {
      continue;
    }
    targets.push(target);
  }
  return orderTargetsForCheck(targets);
}

function checkAmcheck(
  indexName: string,
  tableName: string,
  user: string,
  database: string,
): AmcheckFailure | null {
  const result = runPsql(buildAmcheckCallSql(indexName, true), user, database);
  if (result.ok) {
    return null;
  }
  return {
    indexName,
    tableName,
    message: parseAmcheckErrorMessage(`${result.stderr}\n${result.stdout}`),
  };
}

function countDuplicates(target: IndexTarget, user: string, database: string): number {
  const sql = buildDuplicateCountSql(target);
  if (sql === null) {
    return 0;
  }
  const rows = runPsqlTuplesOnly(sql, user, database);
  return Number(rows[0] ?? "0");
}

function listTableColumns(
  schemaName: string,
  tableName: string,
  user: string,
  database: string,
): string[] {
  return runPsqlTuplesOnly(buildTableColumnsSql(schemaName, tableName), user, database);
}

function listCorruptOrphans(
  user: string,
  database: string,
): Array<{ oid: number; relname: string; relnatts: number; attrCount: number }> {
  const rows = runPsqlTuplesOnly(buildListOrphanTempRelationsSql(), user, database);
  const out: Array<{ oid: number; relname: string; relnatts: number; attrCount: number }> = [];
  for (const line of rows) {
    const [oid, relname, relnatts, attrCount] = line.split("\t");
    if (relname === undefined) {
      continue;
    }
    const parsed = {
      oid: Number(oid),
      relname,
      relnatts: Number(relnatts),
      attrCount: Number(attrCount),
    };
    if (parsed.attrCount === 0 && parsed.relnatts > 0) {
      out.push(parsed);
    }
  }
  return out;
}

function collectReport(user: string, database: string, quick: boolean): HealthReport {
  ensureAmcheck(user, database);
  const targets = listTargets(user, database, quick);
  const amcheckFailures: AmcheckFailure[] = [];
  const duplicateGroups: DuplicateGroup[] = [];
  const preferNewestByColumnByTable: Record<string, string | null> = {};

  for (const target of targets) {
    const failure = checkAmcheck(target.indexName, target.tableName, user, database);
    if (failure !== null) {
      amcheckFailures.push(failure);
      console.error(`amcheck FAIL ${target.indexName}: ${failure.message}`);
    }

    if (target.kind === "unique") {
      const dupCount = countDuplicates(target, user, database);
      if (dupCount > 0) {
        duplicateGroups.push({
          tableName: target.tableName,
          indexName: target.indexName,
          keyColumns: target.keyColumns,
          duplicateRowCount: dupCount,
        });
        console.error(
          `duplicates ${target.tableName}.${target.indexName}: ${dupCount} excess group-rows`,
        );
      }
      if (
        (dupCount > 0 || failure !== null) &&
        !(target.tableName in preferNewestByColumnByTable)
      ) {
        const cols = listTableColumns(target.schemaName, target.tableName, user, database);
        preferNewestByColumnByTable[target.tableName] = resolveDedupePreferColumn(cols);
      }
    }
  }

  const systemCatalogFailures: AmcheckFailure[] = [];
  for (const indexName of DEFAULT_INDEX_HEALTH_CONFIG.systemCatalogIndexes) {
    const failure = checkAmcheck(indexName, "pg_class", user, database);
    if (failure !== null) {
      systemCatalogFailures.push(failure);
      console.error(`amcheck FAIL ${indexName}: ${failure.message}`);
    }
  }

  const orphanTempRelations = listCorruptOrphans(user, database);
  for (const orphan of orphanTempRelations) {
    console.error(
      `orphan temp corrupt oid=${orphan.oid} relname=${orphan.relname} relnatts=${orphan.relnatts} attrs=${orphan.attrCount}`,
    );
  }

  return buildHealthReport({
    checkedIndexCount: targets.length + DEFAULT_INDEX_HEALTH_CONFIG.systemCatalogIndexes.length,
    amcheckFailures,
    duplicateGroups,
    orphanTempRelations,
    systemCatalogFailures,
    preferNewestByColumnByTable,
  });
}

function applyRepair(action: RepairAction, user: string, database: string): void {
  switch (action.type) {
    case "drop_orphan_temp_relation": {
      console.log(`repair: catalog-clean orphan temp ${action.relname} (oid=${action.oid})`);
      const result = runPsql(buildDropOrphanTempRelationSql(action.oid), user, database);
      if (!result.ok) {
        throw new Error(`orphan cleanup failed: ${result.stderr}`);
      }
      return;
    }
    case "reindex_system_catalogs": {
      for (const sql of buildReindexSystemCatalogsSql()) {
        console.log(`repair: ${sql}`);
        const result = runPsql(sql, user, database);
        if (!result.ok) {
          throw new Error(`system catalog REINDEX failed: ${result.stderr}`);
        }
      }
      return;
    }
    case "dedupe_unique_heap": {
      const target: IndexTarget = {
        schemaName: "public",
        tableName: action.tableName,
        indexName: action.indexName,
        kind: "unique",
        keyColumns: action.keyColumns,
      };
      const sql = buildDedupeUniqueHeapSql(target, action.preferNewestByColumn);
      if (sql === null) {
        return;
      }
      console.log(
        `repair: dedupe ${action.tableName} via ${action.indexName} prefer=${action.preferNewestByColumn ?? "ctid"}`,
      );
      const result = runPsql(sql, user, database);
      if (!result.ok) {
        throw new Error(`dedupe failed: ${result.stderr}`);
      }
      console.log(`repair: dedupe deleted_rows=${result.stdout.trim()}`);
      return;
    }
    case "reindex_index": {
      const sql = buildReindexIndexSql(action.indexName);
      console.log(`repair: ${sql} (table=${action.tableName})`);
      const result = runPsql(sql, user, database);
      if (!result.ok) {
        throw new Error(`REINDEX failed for ${action.indexName}: ${result.stderr}`);
      }
      return;
    }
    default: {
      const _exhaustive: never = action;
      throw new Error(`unknown repair action: ${JSON.stringify(_exhaustive)}`);
    }
  }
}

function main(): void {
  const options = parseArgs(process.argv.slice(2));
  const { user, database } = loadEnv();

  console.log(
    `index-health start container=${LOCAL_CONTAINER_NAME} db=${database} repair=${options.repair} quick=${options.quick}`,
  );

  const report = collectReport(user, database, options.quick);
  console.log(summarizeHealthReport(report));

  if (report.healthy) {
    process.exit(0);
  }

  if (!options.repair) {
    console.error(
      "index-health: issues found. Re-run with --repair to fix via REINDEX (no DROP INDEX).",
    );
    process.exit(1);
  }

  for (const action of report.repairActions) {
    applyRepair(action, user, database);
  }

  console.log("index-health: re-verifying after repair...");
  const after = collectReport(user, database, options.quick);
  console.log(summarizeHealthReport(after));
  if (!after.healthy) {
    console.error("index-health: still unhealthy after repair");
    process.exit(2);
  }
  console.log("index-health: repair succeeded");
  process.exit(0);
}

main();
