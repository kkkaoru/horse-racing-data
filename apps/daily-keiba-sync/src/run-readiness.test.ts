// Runs with bun; native SQLite verifies the read-only readiness query.
import { readFile } from "node:fs/promises";
import { Miniflare } from "miniflare";
import { afterAll, beforeAll, beforeEach, expect, test } from "vitest";
import { getRunReadiness } from "./run-readiness";
import { createRun, markParsed } from "./state";

const runtime: Miniflare = new Miniflare({
  compatibilityDate: "2026-06-18",
  d1Databases: { DB: "readiness-test" },
  modules: true,
  script: "export default {}",
});
const database: Promise<D1Database> = runtime.getD1Database("DB");
const now: Date = new Date("2026-09-16T00:00:00Z");

beforeAll(async () => {
  const db: D1Database = await database;
  const migrations: readonly string[] = await Promise.all(
    [
      "0001_initial.sql",
      "0002_catalog_index.sql",
      "0003_acquisition_window.sql",
      "0005_acquisition_data_spec.sql",
    ].map((name) => readFile(new URL(`../migrations/${name}`, import.meta.url), "utf8")),
  );
  await db.exec(
    migrations
      .join("\n")
      .split("\n")
      .filter((line) => !line.trimStart().startsWith("--"))
      .join(" "),
  );
});
afterAll(async () => {
  await runtime.dispose();
});
beforeEach(async () => {
  const db: D1Database = await database;
  await db.exec("delete from sync_run_tables; delete from sync_runs;");
});

// These fixtures exercise actual D1 statements, not mocked SQL responses.
test("reports committed Catalog independently of a failed backup and preserves legacy errors", async () => {
  const db: D1Database = await database;
  const { run } = await createRun(db, "jv", "20260916", "manual", 1, now, true);
  await markParsed(
    db,
    run.run_id,
    1,
    1,
    [
      {
        table_name: "jvd_ra",
        staging_key: "stage",
        source_records: 1,
        catalog_status: "succeeded",
        neon_status: "failed_permanent",
        partitions: [],
      },
    ],
    now,
  );
  await db
    .prepare(
      "update sync_runs set status = 'neon_failed', error_stage = 'neon-schema' where run_id = ?",
    )
    .bind(run.run_id)
    .run();
  const before = await db.prepare("select * from sync_runs").all();
  const result = await getRunReadiness(db, run.run_id);
  expect(result?.catalog_ready).toBe(true);
  expect(result?.neon_backup_complete).toBe(false);
  expect(result?.status).toBe("neon_failed");
  expect(result?.error_stage).toBe("neon-schema");
  expect(result).not.toHaveProperty("catalog_ready_flag");
  expect(result).not.toHaveProperty("neon_backup_complete_flag");
  expect((await db.prepare("select * from sync_runs").all()).results).toStrictEqual(before.results);
});

test("reports backup completion only from all configured table receipts", async () => {
  const db: D1Database = await database;
  const { run } = await createRun(db, "jv", "20260916", "manual", 1, now, true);
  await markParsed(
    db,
    run.run_id,
    2,
    2,
    [
      {
        table_name: "jvd_ra",
        staging_key: "ra",
        source_records: 1,
        catalog_status: "succeeded",
        neon_status: "succeeded",
        partitions: [],
      },
      {
        table_name: "unconfigured",
        staging_key: "other",
        source_records: 1,
        catalog_status: "not_configured",
        neon_status: "not_configured",
        partitions: [],
      },
    ],
    now,
  );
  expect((await getRunReadiness(db, run.run_id))?.catalog_ready).toBe(true);
  expect((await getRunReadiness(db, run.run_id))?.neon_backup_complete).toBe(true);
});

test("does not let legacy aggregate success hide an unfinished Catalog table", async () => {
  const db: D1Database = await database;
  const { run } = await createRun(db, "jv", "20260916", "manual", 1, now, true);
  await markParsed(
    db,
    run.run_id,
    2,
    2,
    [
      {
        table_name: "jvd_ra",
        staging_key: "ra",
        source_records: 1,
        catalog_status: "succeeded",
        neon_status: "succeeded",
        partitions: [],
      },
      {
        table_name: "jvd_se",
        staging_key: "se",
        source_records: 1,
        catalog_status: "index_pending",
        neon_status: "pending",
        partitions: [],
      },
    ],
    now,
  );
  await db
    .prepare("update sync_runs set status = 'succeeded' where run_id = ?")
    .bind(run.run_id)
    .run();
  expect((await getRunReadiness(db, run.run_id))?.catalog_ready).toBe(false);
  expect((await getRunReadiness(db, run.run_id))?.neon_backup_complete).toBe(false);
});

test("rejects missing receipts even when every remaining receipt succeeded", async () => {
  const db: D1Database = await database;
  const { run } = await createRun(db, "jv", "20260916", "manual", 1, now, true);
  await markParsed(
    db,
    run.run_id,
    1,
    1,
    [
      {
        table_name: "jvd_ra",
        staging_key: "ra",
        source_records: 1,
        catalog_status: "succeeded",
        neon_status: "pending",
        partitions: [],
      },
    ],
    now,
  );
  await db
    .prepare("update sync_runs set catalog_tables = 2 where run_id = ?")
    .bind(run.run_id)
    .run();
  expect((await getRunReadiness(db, run.run_id))?.catalog_ready).toBe(false);
});

test("rejects unknown Catalog state despite a matching successful-table count", async () => {
  const db: D1Database = await database;
  const { run } = await createRun(db, "jv", "20260916", "manual", 1, now, true);
  await markParsed(
    db,
    run.run_id,
    2,
    2,
    [
      {
        table_name: "jvd_ra",
        staging_key: "ra",
        source_records: 1,
        catalog_status: "succeeded",
        neon_status: "succeeded",
        partitions: [],
      },
      {
        table_name: "jvd_se",
        staging_key: "se",
        source_records: 1,
        catalog_status: "unknown",
        neon_status: "pending",
        partitions: [],
      },
    ],
    now,
  );
  await db
    .prepare("update sync_runs set catalog_tables = 1 where run_id = ?")
    .bind(run.run_id)
    .run();
  expect((await getRunReadiness(db, run.run_id))?.catalog_ready).toBe(false);
});

test("does not infer Catalog publication from an empty or unconfigured plan", async () => {
  const db: D1Database = await database;
  const { run } = await createRun(db, "jv", "20260916", "manual", 1, now, true);
  expect((await getRunReadiness(db, run.run_id))?.catalog_ready).toBe(false);
  await markParsed(
    db,
    run.run_id,
    1,
    1,
    [
      {
        table_name: "other",
        staging_key: "other",
        source_records: 1,
        catalog_status: "not_configured",
        neon_status: "not_configured",
        partitions: [],
      },
    ],
    now,
  );
  expect((await getRunReadiness(db, run.run_id))?.catalog_ready).toBe(false);
  expect((await getRunReadiness(db, run.run_id))?.neon_backup_complete).toBe(false);
});

test("returns null for an absent run", async () => {
  expect(await getRunReadiness(await database, "missing")).toBeNull();
});
