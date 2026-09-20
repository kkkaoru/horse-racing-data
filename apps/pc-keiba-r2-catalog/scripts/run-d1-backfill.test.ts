// Runs with bun via Vitest; temporary local artifacts with fake D1 and Catalog I/O.
import { chmod, mkdtemp, readFile, rm, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, test, vi } from "vitest";
import { runD1Backfill, runD1BackfillCli, type D1RunnerOptions } from "./run-d1-backfill";

const folders: string[] = [];
const originalArgv: string[] = process.argv;
const schema = [{ name: "v", type: "INTEGER", notnull: 1, pk: 0, dflt_value: null }];
const row = { row_key: "1", payload: '{"v":{"type":"integer","value":"1"}}' };
const temporaryOptions = async (): Promise<D1RunnerOptions> => {
  const directory: string = await mkdtemp(join(tmpdir(), "d1-runner-test-"));
  folders.push(directory);
  return {
    accountId: "account",
    token: "example",
    databaseName: "daily-keiba-sync",
    tableName: "events",
    snapshotId: "test",
    directory,
    batches: 2,
  };
};
const source = (columns: Record<string, unknown>[] = schema) =>
  vi.fn(async (_input: string, init: RequestInit): Promise<Response> => {
    if (init.method === "GET")
      return Response.json({
        success: true,
        result: [{ name: "daily-keiba-sync", uuid: "database" }],
      });
    const text: string = String(init.body);
    return Response.json({
      success: true,
      result: [
        {
          success: true,
          results: text.includes("PRAGMA")
            ? columns
            : text.includes("-9223372036854775808")
              ? [row]
              : [],
        },
      ],
    });
  });
const tableFolder = (options: D1RunnerOptions): string =>
  join(options.directory, options.snapshotId, options.databaseName, options.tableName);

afterEach(async () => {
  process.argv = originalArgv;
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  await Promise.all(
    folders.splice(0).map(async (folder) => {
      await rm(folder, { recursive: true, force: true });
    }),
  );
});

test("captures and resumes a table with preserved source schema", async () => {
  const options: D1RunnerOptions = await temporaryOptions();
  const dependencies = {
    fetchImpl: source(),
    publishFile: vi.fn().mockResolvedValue({ rows: 1, reconciled: true }),
  };
  const first = await runD1Backfill(options, dependencies);
  expect(first.phase).toBe("copied");
  expect(first.copiedRows).toBe(1);
  expect(await runD1Backfill(options, dependencies)).toStrictEqual(first);
  expect(dependencies.publishFile).toHaveBeenCalledTimes(1);
  expect(await readFile(join(tableFolder(options), "schema.json"), "utf8")).toContain(
    '"defaultValue":null',
  );
  // Lost local progress can replay the same content-addressed artifact, not mutate it.
  await unlink(join(tableFolder(options), "checkpoint.json"));
  expect((await runD1Backfill(options, dependencies)).copiedRows).toBe(1);
});

test("rejects a second writer and leaves the existing lock untouched", async () => {
  const options: D1RunnerOptions = await temporaryOptions();
  const dependencies = {
    fetchImpl: source(),
    publishFile: vi.fn().mockResolvedValue({ rows: 1, reconciled: true }),
  };
  await runD1Backfill(options, dependencies);
  const path: string = join(tableFolder(options), "writer.lock");
  await writeFile(path, '{"pid":123}');
  await expect(runD1Backfill(options, dependencies)).rejects.toThrow("EEXIST");
  expect(await readFile(path, "utf8")).toBe('{"pid":123}');
});

test("retains and retries the original pending artifact after a publisher outage", async () => {
  const options: D1RunnerOptions = await temporaryOptions();
  const dependencies = {
    fetchImpl: source(),
    publishFile: vi
      .fn()
      .mockRejectedValueOnce(new Error("offline"))
      .mockResolvedValue({ rows: 1, reconciled: true }),
  };
  await expect(runD1Backfill(options, dependencies)).rejects.toThrow("offline");
  expect((await runD1Backfill(options, dependencies)).copiedRows).toBe(1);
  expect(dependencies.publishFile.mock.calls[0]).toStrictEqual(
    dependencies.publishFile.mock.calls[1],
  );
});

test.each([null, { rows: "1", reconciled: true }, { rows: 1, reconciled: false }])(
  "rejects unverified publication receipts",
  async (receipt) => {
    await expect(
      runD1Backfill(await temporaryOptions(), {
        fetchImpl: source(),
        publishFile: vi.fn().mockResolvedValue(receipt),
      }),
    ).rejects.toThrow("Invalid Catalog publication receipt");
  },
);

test.each<Partial<D1RunnerOptions>>([
  { databaseName: "unrelated-db" },
  { snapshotId: "../outside" },
  { tableName: "invalid table" },
  { tableName: "_cf_KV" },
  { tableName: "sqlite_schema" },
  { batches: 0 },
  { batches: 1.5 },
  { batches: 1001 },
  { token: "" },
  { accountId: "" },
])("rejects unsafe configuration without reading D1", async (override) => {
  const fetchImpl = source();
  await expect(
    runD1Backfill(
      { ...(await temporaryOptions()), ...override },
      { fetchImpl, publishFile: vi.fn() },
    ),
  ).rejects.toThrow("Invalid or unauthorized");
  expect(fetchImpl).not.toHaveBeenCalled();
});

test.each([
  null,
  {},
  { success: false },
  { success: true, result: {} },
  { success: true, result: [null] },
  { success: true, result: [{ name: "daily-keiba-sync", uuid: 1 }] },
])("rejects invalid discovery responses", async (body) => {
  await expect(
    runD1Backfill(await temporaryOptions(), {
      fetchImpl: vi.fn().mockResolvedValue(Response.json(body)),
      publishFile: vi.fn(),
    }),
  ).rejects.toThrow();
});

test("rejects discovery HTTP errors", async () => {
  await expect(
    runD1Backfill(await temporaryOptions(), {
      fetchImpl: vi.fn().mockResolvedValue(new Response("", { status: 403 })),
      publishFile: vi.fn(),
    }),
  ).rejects.toThrow("discovery failed");
});

test.each([
  { columns: [] },
  { columns: [{}] },
  { columns: [{ name: "v" }] },
  { columns: [{ name: "v", type: "INTEGER" }] },
  { columns: [{ name: "v", type: "INTEGER", notnull: 1 }] },
  { columns: [{ name: "v", type: "INTEGER", notnull: 1, pk: 0, dflt_value: 1 }] },
])("rejects missing or malformed table schema", async ({ columns }) => {
  await expect(
    runD1Backfill(await temporaryOptions(), { fetchImpl: source(columns), publishFile: vi.fn() }),
  ).rejects.toThrow();
});

test("stops when source schema changes", async () => {
  const options: D1RunnerOptions = await temporaryOptions();
  const publishFile = vi.fn().mockResolvedValue({ rows: 1, reconciled: true });
  await runD1Backfill(options, { fetchImpl: source(), publishFile });
  await expect(
    runD1Backfill(options, {
      fetchImpl: source([{ name: "v", type: "TEXT", notnull: 1, pk: 0, dflt_value: "'default'" }]),
      publishFile,
    }),
  ).rejects.toThrow("source schema changed");
});

test("rejects corrupt checkpoints even when schema metadata matches", async () => {
  const options: D1RunnerOptions = await temporaryOptions();
  const dependencies = {
    fetchImpl: source(),
    publishFile: vi.fn().mockResolvedValue({ rows: 1, reconciled: true }),
  };
  await runD1Backfill(options, dependencies);
  await writeFile(join(tableFolder(options), "checkpoint.json"), "null");
  await expect(runD1Backfill(options, dependencies)).rejects.toThrow("Checkpoint schema signature");
  await writeFile(join(tableFolder(options), "checkpoint.json"), "[]");
  await expect(runD1Backfill(options, dependencies)).rejects.toThrow("Checkpoint schema signature");
  await writeFile(join(tableFolder(options), "checkpoint.json"), "{}");
  await expect(runD1Backfill(options, dependencies)).rejects.toThrow("Checkpoint schema signature");
});

test.each(["digest", "path"])("refuses changed pending artifact %s", async (field) => {
  const options: D1RunnerOptions = await temporaryOptions();
  const dependencies = {
    fetchImpl: source(),
    publishFile: vi.fn().mockRejectedValue(new Error("offline")),
  };
  await expect(runD1Backfill(options, dependencies)).rejects.toThrow("offline");
  const checkpoint: unknown = JSON.parse(
    await readFile(join(tableFolder(options), "checkpoint.json"), "utf8"),
  );
  if (typeof checkpoint !== "object" || checkpoint === null || !("pending" in checkpoint))
    throw new Error("Missing checkpoint fixture");
  const pending = checkpoint.pending;
  if (
    typeof pending !== "object" ||
    pending === null ||
    !("path" in pending) ||
    typeof pending.path !== "string"
  )
    throw new Error("Missing pending fixture");
  if (field === "path") pending.path = "/outside";
  else {
    await chmod(pending.path, 0o600);
    await writeFile(pending.path, "corrupted");
  }
  await writeFile(join(tableFolder(options), "checkpoint.json"), JSON.stringify(checkpoint));
  await expect(runD1Backfill(options, dependencies)).rejects.toThrow(
    field === "path" ? "outside" : "digest changed",
  );
});

test.each([0, 1])(
  "CLI executes the bounded publisher and checks exit code %i",
  async (exitCode) => {
    const options: D1RunnerOptions = await temporaryOptions();
    process.argv = [
      "bun",
      "runner",
      "--database",
      options.databaseName,
      "--table",
      options.tableName,
      "--snapshot",
      options.snapshotId,
      "--directory",
      options.directory,
      "--batches",
      "1",
    ];
    vi.stubEnv("R2_ACCOUNT_ID", "account");
    vi.stubEnv("CLOUDFLARE_DEBUG_TOKEN", "example");
    vi.stubGlobal("fetch", source());
    vi.stubGlobal("Bun", {
      spawn: vi.fn().mockReturnValue({
        stdout: new Response('{"rows":1,"reconciled":true}').body,
        exited: Promise.resolve(exitCode),
      }),
    });
    const log = vi.spyOn(console, "log").mockImplementation(() => {});
    if (exitCode === 0) {
      await runD1BackfillCli();
      expect(log).toHaveBeenCalledOnce();
    } else await expect(runD1BackfillCli()).rejects.toThrow("publisher failed");
  },
);

test.each([{ args: [] }, { args: ["--database"] }])(
  "CLI rejects incomplete arguments and missing configuration",
  async ({ args }) => {
    process.argv = ["bun", "runner", ...args];
    vi.stubEnv("R2_ACCOUNT_ID", undefined);
    vi.stubEnv("CLOUDFLARE_DEBUG_TOKEN", undefined);
    await expect(runD1BackfillCli()).rejects.toThrow("Invalid or unauthorized");
  },
);
