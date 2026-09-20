// Runs with bun; single-writer, bounded journal export. Never installs DDL or prunes source rows.
import { mkdir, open, readFile, rename, unlink, writeFile } from "node:fs/promises";
import { resolve, join } from "node:path";
import {
  exportD1CaptureStep,
  parseD1CaptureExportState,
  type D1CaptureExportConfig,
  type D1CaptureExportState,
} from "../src/d1-capture-export";
import { prepareD1CaptureInstall, type D1CaptureInstallInput } from "../src/d1-capture-install";
import { buildD1CapturePageQuery } from "../src/d1-capture-reader";
import { queryD1Snapshot, type D1SnapshotClient } from "../src/d1-snapshot";

export interface D1CaptureRunnerPlan extends D1CaptureInstallInput {
  schemaHash: string;
}
export interface D1CaptureRunnerOptions {
  accountId: string;
  token: string;
  databaseName: string;
  databaseId: string;
  plans: readonly D1CaptureRunnerPlan[];
  directory: string;
  batches: number;
  pageSize: number;
}
export interface D1CaptureRunnerDependencies {
  fetchImpl: D1SnapshotClient["fetchImpl"];
  publishFile: (path: string) => Promise<unknown>;
}
const DATABASES: ReadonlySet<string> = new Set([
  "daily-keiba-sync",
  "sync-realtime-data-hot-v2",
  "venue-weather-db",
  "finish-position-cron-db",
  "sync-realtime-data-features-db",
  "sync-realtime-data",
]);
const MAX_BATCHES: number = 1000;
const REQUEST_TIMEOUT: number = 60000;
const PUBLISH_TIMEOUT: number = 300000;
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const list = (value: unknown): value is readonly unknown[] => Array.isArray(value);

const readOptional = async (path: string): Promise<string | null> => {
  try {
    return await readFile(path, "utf8");
  } catch (error) {
    if (record(error) && error.code === "ENOENT") return null;
    throw error;
  }
};

const verifySource = async (
  options: D1CaptureRunnerOptions,
  client: D1SnapshotClient,
): Promise<void> => {
  const response = await client.fetchImpl(
    `https://api.cloudflare.com/client/v4/accounts/${encodeURIComponent(client.accountId)}/d1/database/${encodeURIComponent(client.databaseId)}`,
    {
      method: "GET",
      headers: { Authorization: `Bearer ${client.token}` },
      signal: AbortSignal.timeout(REQUEST_TIMEOUT),
    },
  );
  const body: unknown = await response.json();
  if (
    !response.ok ||
    !record(body) ||
    body.success !== true ||
    !record(body.result) ||
    body.result.name !== options.databaseName
  )
    throw new Error("Capture database ownership check failed");
  for (const plan of options.plans) {
    const verified = await prepareD1CaptureInstall(plan, (query) => queryD1Snapshot(client, query));
    if (!verified.ddlVerified || verified.plan.schemaHash !== plan.schemaHash)
      throw new Error("Capture source DDL is not verified");
  }
};

export const runD1Capture = async (
  options: D1CaptureRunnerOptions,
  dependencies: D1CaptureRunnerDependencies,
): Promise<D1CaptureExportState> => {
  if (
    !DATABASES.has(options.databaseName) ||
    !options.accountId ||
    !options.token ||
    !options.directory ||
    options.plans.length === 0 ||
    !Number.isInteger(options.batches) ||
    options.batches < 1 ||
    options.batches > MAX_BATCHES
  )
    throw new Error("Invalid capture runner configuration");
  const config: D1CaptureExportConfig = {
    databaseName: options.databaseName,
    databaseId: options.databaseId,
    registrations: options.plans.map((plan) => ({
      table: plan.table,
      captureId: plan.captureId,
      schemaHash: plan.schemaHash,
    })),
  };
  const initial: D1CaptureExportState = {
    databaseName: options.databaseName,
    databaseId: options.databaseId,
    afterSequence: "0",
    pending: null,
  };
  parseD1CaptureExportState(initial, config);
  buildD1CapturePageQuery({ afterSequence: "0", throughSequence: "0", limit: options.pageSize });
  const folder = resolve(options.directory, options.databaseId);
  await mkdir(join(folder, "batches"), { recursive: true, mode: 0o700 });
  const lockPath = join(folder, "writer.lock");
  const lock = await open(lockPath, "wx", 0o600);
  try {
    await lock.writeFile(JSON.stringify({ pid: process.pid, databaseId: options.databaseId }));
    const statePath = join(folder, "checkpoint.json");
    const stored = await readOptional(statePath);
    const progress = {
      state: stored === null ? initial : parseD1CaptureExportState(JSON.parse(stored), config),
      batches: 0,
    };
    const client: D1SnapshotClient = {
      accountId: options.accountId,
      databaseId: options.databaseId,
      token: options.token,
      fetchImpl: dependencies.fetchImpl,
    };
    while (progress.batches < options.batches) {
      const previous = progress.state.afterSequence;
      progress.state = await exportD1CaptureStep({
        config,
        state: progress.state,
        pageSize: options.pageSize,
        dependencies: {
          load: async (afterSequence) => {
            await verifySource(options, client);
            const boundary = await queryD1Snapshot(client, {
              sql: 'SELECT CAST(COALESCE(MAX(sequence), 0) AS TEXT) AS sequence FROM "__pc_keiba_catalog_cdc_v1"',
              params: [],
            });
            const first = boundary[0];
            if (boundary.length !== 1 || first === undefined || typeof first.sequence !== "string")
              throw new Error("Invalid capture journal boundary");
            const request = {
              afterSequence,
              throughSequence: first.sequence,
              limit: options.pageSize,
            };
            const rows = await queryD1Snapshot(client, buildD1CapturePageQuery(request));
            await verifySource(options, client);
            return { throughSequence: first.sequence, rows };
          },
          retainArtifact: async (artifact) => {
            const path = join(folder, "batches", `${artifact.batchId}.json`);
            const existing = await readOptional(path);
            if (existing === null)
              await writeFile(path, artifact.serialized, { flag: "wx", mode: 0o400 });
            else if (existing !== artifact.serialized)
              throw new Error("Immutable capture artifact collision");
          },
          checkpoint: async (state) => {
            await writeFile(`${statePath}.next`, JSON.stringify(state), { mode: 0o600 });
            await rename(`${statePath}.next`, statePath);
          },
          publish: async (artifact) => {
            const path = join(folder, "batches", `${artifact.batchId}.json`);
            if ((await readFile(path, "utf8")) !== artifact.serialized)
              throw new Error("Pending capture bytes changed");
            return dependencies.publishFile(path);
          },
        },
      });
      progress.batches += 1;
      if (progress.state.afterSequence === previous) break;
    }
    return progress.state;
  } finally {
    await lock.close();
    await unlink(lockPath);
  }
};

const required = (value: unknown): string => {
  if (typeof value !== "string" || value.length === 0)
    throw new Error("Missing capture runner argument");
  return value;
};
const parsePlan = (value: unknown): D1CaptureRunnerPlan => {
  if (!record(value)) throw new Error("Invalid capture plan configuration");
  return {
    table: required(value.table),
    captureId: required(value.captureId),
    expectedDefinitionHash: required(value.expectedDefinitionHash),
    expectedPlanHash: required(value.expectedPlanHash),
    schemaHash: required(value.schemaHash),
  };
};

export const runD1CaptureCli = async (): Promise<void> => {
  if (process.argv.length !== 4 || process.argv[2] !== "--config")
    throw new Error("Expected --config path");
  const config: unknown = JSON.parse(await readFile(required(process.argv[3]), "utf8"));
  if (
    !record(config) ||
    !list(config.plans) ||
    typeof config.batches !== "number" ||
    typeof config.pageSize !== "number"
  )
    throw new Error("Invalid capture runner configuration file");
  const state = await runD1Capture(
    {
      accountId: required(process.env.R2_ACCOUNT_ID),
      token: required(process.env.CLOUDFLARE_DEBUG_TOKEN),
      databaseName: required(config.databaseName),
      databaseId: required(config.databaseId),
      plans: config.plans.map(parsePlan),
      directory: required(config.directory),
      batches: config.batches,
      pageSize: config.pageSize,
    },
    {
      fetchImpl: fetch,
      publishFile: async (path) => {
        const child = Bun.spawn(["uv", "run", "scripts/publish_d1_capture.py", "--batch", path], {
          stdout: "pipe",
          stderr: "ignore",
          timeout: PUBLISH_TIMEOUT,
        });
        const [output, status] = await Promise.all([
          new Response(child.stdout).text(),
          child.exited,
        ]);
        if (status !== 0) throw new Error("Capture publication failed; retain pending artifact");
        return JSON.parse(output);
      },
    },
  );
  console.log(
    JSON.stringify({
      databaseName: state.databaseName,
      afterSequence: state.afterSequence,
      pending: state.pending !== null,
      promoted: false,
    }),
  );
};

if (import.meta.main) await runD1CaptureCli();
