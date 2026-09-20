// Runs with bun; one bounded, resumable application-table snapshot per invocation.
import { createHash } from "node:crypto";
import { mkdir, open, readFile, rename, unlink, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { copyD1BackfillStep, parseD1BackfillState, type D1BackfillState } from "../src/d1-backfill";
import {
  buildD1SnapshotPageQuery,
  parseD1SnapshotPage,
  queryD1Snapshot,
  type D1SnapshotClient,
} from "../src/d1-snapshot";

interface D1ColumnSchema {
  name: string;
  type: string;
  notNull: number;
  primaryKey: number;
  defaultValue: string | null;
}

export interface D1RunnerOptions {
  accountId: string;
  token: string;
  databaseName: string;
  tableName: string;
  snapshotId: string;
  directory: string;
  batches: number;
}

export interface D1RunnerDependencies {
  fetchImpl(input: string, init: RequestInit): Promise<Response>;
  publishFile(path: string): Promise<unknown>;
}

const DATABASES: readonly string[] = [
  "daily-keiba-sync",
  "sync-realtime-data-hot-v2",
  "venue-weather-db",
  "finish-position-cron-db",
  "sync-realtime-data-features-db",
  "sync-realtime-data",
];
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const digest = (text: string): string => createHash("sha256").update(text).digest("hex");

const readOptional = async (path: string): Promise<string | null> => {
  try {
    return await readFile(path, "utf8");
  } catch (error) {
    if (isRecord(error) && error.code === "ENOENT") return null;
    throw error;
  }
};

const atomicCheckpoint = async (path: string, content: string): Promise<void> => {
  await writeFile(`${path}.next`, content, { mode: 0o600 });
  await rename(`${path}.next`, path);
};

const columnSchema = (row: Record<string, unknown>): D1ColumnSchema => {
  if (
    typeof row.name !== "string" ||
    typeof row.type !== "string" ||
    typeof row.notnull !== "number" ||
    typeof row.pk !== "number" ||
    (row.dflt_value !== null && typeof row.dflt_value !== "string")
  )
    throw new Error("Invalid D1 source schema");
  return {
    name: row.name,
    type: row.type,
    notNull: row.notnull,
    primaryKey: row.pk,
    defaultValue: row.dflt_value,
  };
};

export const runD1Backfill = async (
  options: D1RunnerOptions,
  dependencies: D1RunnerDependencies,
): Promise<D1BackfillState> => {
  if (
    !DATABASES.includes(options.databaseName) ||
    !/^[a-zA-Z0-9_-]{1,128}$/u.test(options.snapshotId) ||
    !/^[a-zA-Z0-9_]{1,128}$/u.test(options.tableName) ||
    options.tableName.startsWith("_cf_") ||
    options.tableName.startsWith("sqlite_") ||
    !Number.isInteger(options.batches) ||
    options.batches < 1 ||
    options.batches > 1000 ||
    !options.accountId ||
    !options.token
  )
    throw new Error("Invalid or unauthorized D1 snapshot configuration");
  const response: Response = await dependencies.fetchImpl(
    `https://api.cloudflare.com/client/v4/accounts/${encodeURIComponent(options.accountId)}/d1/database?per_page=100`,
    {
      method: "GET",
      headers: { Authorization: `Bearer ${options.token}` },
      signal: AbortSignal.timeout(60_000),
    },
  );
  if (!response.ok) throw new Error("D1 database discovery failed");
  const databases: unknown = await response.json();
  if (!isRecord(databases) || databases.success !== true || !Array.isArray(databases.result))
    throw new Error("Invalid D1 database discovery response");
  const database: unknown = databases.result.find(
    (item: unknown) => isRecord(item) && item.name === options.databaseName,
  );
  if (!isRecord(database) || typeof database.uuid !== "string")
    throw new Error("Requested application database was not found");
  const client: D1SnapshotClient = {
    accountId: options.accountId,
    databaseId: database.uuid,
    token: options.token,
    fetchImpl: dependencies.fetchImpl,
  };
  const schema: D1ColumnSchema[] = (
    await queryD1Snapshot(client, { sql: `PRAGMA table_info("${options.tableName}")`, params: [] })
  ).map(columnSchema);
  if (schema.length === 0) throw new Error("Requested application table was not found");
  const schemaSignature: string = digest(JSON.stringify(schema));
  const folder: string = resolve(
    options.directory,
    options.snapshotId,
    options.databaseName,
    options.tableName,
  );
  await mkdir(join(folder, "batches"), { recursive: true, mode: 0o700 });
  // Local migration orchestration only; this is not a replacement for production D1 leases.
  const lockPath: string = join(folder, "writer.lock");
  const lock = await open(lockPath, "wx", 0o600);
  try {
    await lock.writeFile(JSON.stringify({ pid: process.pid }));
    const schemaPath: string = join(folder, "schema.json");
    const schemaDocument: string = JSON.stringify({ schemaSignature, schema });
    const existingSchema: string | null = await readOptional(schemaPath);
    if (existingSchema !== null && existingSchema !== schemaDocument)
      throw new Error("D1 source schema changed during snapshot capture");
    if (existingSchema === null)
      await writeFile(schemaPath, schemaDocument, { flag: "wx", mode: 0o400 });
    const statePath: string = join(folder, "checkpoint.json");
    const existingState: string | null = await readOptional(statePath);
    const initial: D1BackfillState = {
      snapshotId: options.snapshotId,
      databaseName: options.databaseName,
      tableName: options.tableName,
      afterRowId: null,
      copiedRows: 0,
      pending: null,
      phase: "copying",
    };
    const stored: unknown = existingState === null ? null : JSON.parse(existingState);
    if (existingState !== null && (!isRecord(stored) || stored.schemaSignature !== schemaSignature))
      throw new Error("Checkpoint schema signature does not match source");
    const progress = {
      state: existingState === null ? initial : parseD1BackfillState(stored, initial),
      batches: 0,
    };
    while (progress.state.phase === "copying" && progress.batches < options.batches) {
      progress.state = await copyD1BackfillStep({
        state: progress.state,
        batchRows: 50_000,
        batchBytes: 32 * 1024 * 1024,
        pageRows: 1000,
        dependencies: {
          read: async (afterRowId, limit) => {
            const request = { table: options.tableName, columns: schema, afterRowId, limit };
            return parseD1SnapshotPage(
              await queryD1Snapshot(client, buildD1SnapshotPageQuery(request)),
              request,
            );
          },
          stage: async (batch) => {
            const content: string = JSON.stringify(batch);
            const hash: string = digest(content);
            const path: string = join(folder, "batches", `${hash}.json`);
            const existing: string | null = await readOptional(path);
            if (existing === null) await writeFile(path, content, { flag: "wx", mode: 0o400 });
            else if (digest(existing) !== hash)
              throw new Error("Existing immutable D1 batch is corrupt");
            return { path, digest: hash };
          },
          publish: async (artifact) => {
            if (artifact.path !== join(folder, "batches", `${artifact.digest}.json`))
              throw new Error("Pending D1 batch is outside its snapshot directory");
            if (digest(await readFile(artifact.path, "utf8")) !== artifact.digest)
              throw new Error("Pending immutable D1 batch digest changed");
            const receipt: unknown = await dependencies.publishFile(artifact.path);
            if (
              !isRecord(receipt) ||
              typeof receipt.rows !== "number" ||
              receipt.reconciled !== true
            )
              throw new Error("Invalid Catalog publication receipt");
            return { rows: receipt.rows, reconciled: receipt.reconciled };
          },
          checkpoint: async (state) => {
            await atomicCheckpoint(statePath, JSON.stringify({ ...state, schemaSignature }));
          },
        },
      });
      progress.batches += 1;
    }
    return progress.state;
  } finally {
    await lock.close();
    await unlink(lockPath);
  }
};

export const runD1BackfillCli = async (): Promise<void> => {
  const args: Record<string, string> = {};
  for (const [index, arg] of process.argv.slice(2).entries()) {
    if (arg.startsWith("--")) args[arg.slice(2)] = process.argv[index + 3] ?? "";
  }
  const result: D1BackfillState = await runD1Backfill(
    {
      accountId: process.env.R2_ACCOUNT_ID ?? "",
      token: process.env.CLOUDFLARE_DEBUG_TOKEN ?? "",
      databaseName: args.database ?? "",
      tableName: args.table ?? "",
      snapshotId: args.snapshot ?? "",
      directory: args.directory ?? "/tmp/horse-d1-backfill",
      batches: Number(args.batches ?? "1"),
    },
    {
      fetchImpl: fetch,
      publishFile: async (path) => {
        const child = Bun.spawn(
          [
            "uv",
            "run",
            new URL("./publish_d1_snapshot.py", import.meta.url).pathname,
            "--batch",
            path,
          ],
          { stdout: "pipe", stderr: "inherit", timeout: 180_000 },
        );
        const output: string = await new Response(child.stdout).text();
        if ((await child.exited) !== 0)
          throw new Error("Iceberg publisher failed; pending checkpoint retained");
        const receipt: unknown = JSON.parse(output);
        return receipt;
      },
    },
  );
  console.log(
    JSON.stringify({
      database: result.databaseName,
      table: result.tableName,
      rows: result.copiedRows,
      phase: result.phase,
    }),
  );
};

if (import.meta.main) await runD1BackfillCli();
