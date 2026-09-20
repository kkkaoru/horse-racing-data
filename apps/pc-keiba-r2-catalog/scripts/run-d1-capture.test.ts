// Runs with bun via Vitest; filesystem, D1 and publisher boundaries are mocked.
import * as fs from "node:fs/promises";
import type { FileHandle } from "node:fs/promises";
import { beforeEach, afterEach, expect, test, vi } from "vitest";
import { mockDeep } from "vitest-mock-extended";
import * as install from "../src/d1-capture-install";
import {
  runD1Capture,
  runD1CaptureCli,
  type D1CaptureRunnerOptions,
  type D1CaptureRunnerDependencies,
} from "./run-d1-capture";

vi.mock("node:fs/promises", () => ({
  mkdir: vi.fn(),
  open: vi.fn(),
  readFile: vi.fn(),
  writeFile: vi.fn(),
  rename: vi.fn(),
  unlink: vi.fn(),
}));
const files: Map<string, string> = new Map();
const options: D1CaptureRunnerOptions = {
  accountId: "account",
  token: "example",
  databaseName: "venue-weather-db",
  databaseId: "00000000-0000-0000-0000-000000000001",
  plans: [
    {
      table: "items",
      captureId: "test",
      schemaHash: "0".repeat(64),
      expectedDefinitionHash: "0".repeat(64),
      expectedPlanHash: "0".repeat(64),
    },
  ],
  directory: "/capture",
  batches: 2,
  pageSize: 1000,
};
const folder: string = "/capture/00000000-0000-0000-0000-000000000001";
const row: Record<string, unknown> = {
  sequence: "1",
  capture_id: "test",
  table_name: "items",
  schema_hash: "0".repeat(64),
  operation: "insert",
  before_key: null,
  after_key: "2",
  captured_at: "2026-09-16T00:00:00.000Z",
};
const argv: string[] = process.argv;
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const result = (rows: readonly Record<string, unknown>[]): Response =>
  Response.json({ success: true, result: [{ success: true, results: rows }] });
const source = () =>
  vi.fn<D1CaptureRunnerDependencies["fetchImpl"]>(async (_url, init) => {
    if (init.method === "GET")
      return Response.json({ success: true, result: { name: "venue-weather-db" } });
    const query: unknown = JSON.parse(String(init.body));
    if (!record(query) || typeof query.sql !== "string" || !Array.isArray(query.params))
      throw new Error("Unexpected query");
    if (query.sql.includes("MAX(sequence)")) return result([{ sequence: "1" }]);
    return result(query.params[0] === "0" ? [row] : []);
  });
const publisher = () =>
  vi.fn<D1CaptureRunnerDependencies["publishFile"]>(async (path) => {
    const document: unknown = JSON.parse(String(files.get(path)));
    if (!record(document)) throw new Error("Invalid fixture artifact");
    return { batchId: document.batchId, eventCount: 1, lastSequence: "1", reconciled: true };
  });

beforeEach(() => {
  files.clear();
  vi.mocked(fs.mkdir).mockResolvedValue(undefined);
  vi.mocked(fs.open).mockImplementation(async (path) => {
    const key = String(path);
    if (files.has(key)) throw Object.assign(new Error("locked"), { code: "EEXIST" });
    files.set(key, "");
    const handle = mockDeep<FileHandle>();
    handle.writeFile.mockImplementation(async (content) => {
      files.set(key, String(content));
    });
    handle.close.mockResolvedValue(undefined);
    return handle;
  });
  vi.mocked(fs.readFile).mockImplementation(async (path) => {
    const value = files.get(String(path));
    if (value === undefined) throw Object.assign(new Error("missing"), { code: "ENOENT" });
    return value;
  });
  vi.mocked(fs.writeFile).mockImplementation(async (path, content) => {
    files.set(String(path), String(content));
  });
  vi.mocked(fs.rename).mockImplementation(async (from, to) => {
    files.set(String(to), String(files.get(String(from))));
    files.delete(String(from));
  });
  vi.mocked(fs.unlink).mockImplementation(async (path) => {
    files.delete(String(path));
  });
  vi.spyOn(install, "prepareD1CaptureInstall").mockResolvedValue({
    definitionHash: "0".repeat(64),
    ddlVerified: true,
    statements: [],
    plan: { createOutbox: "", triggers: [], schemaHash: "0".repeat(64), planHash: "0".repeat(64) },
  });
});
afterEach(() => {
  process.argv = argv;
  vi.restoreAllMocks();
  vi.clearAllMocks();
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});

test("publishes under an exclusive lock and resumes a completed cursor without rewriting", async () => {
  const dependencies = { fetchImpl: source(), publishFile: publisher() };
  expect((await runD1Capture(options, dependencies)).afterSequence).toBe("1");
  expect((await runD1Capture(options, dependencies)).afterSequence).toBe("1");
  expect(dependencies.publishFile).toHaveBeenCalledTimes(1);
  expect(fs.open).toHaveBeenCalledWith(
    "/capture/00000000-0000-0000-0000-000000000001/writer.lock",
    "wx",
    384,
  );
  expect(files.has(`${folder}/writer.lock`)).toBe(false);
});

test("refuses an existing lock and never deletes it", async () => {
  files.set(`${folder}/writer.lock`, "another owner");
  await expect(
    runD1Capture(options, { fetchImpl: source(), publishFile: publisher() }),
  ).rejects.toThrow("locked");
  expect(files.get(`${folder}/writer.lock`)).toBe("another owner");
  expect(fs.unlink).not.toHaveBeenCalled();
});

test("pending replay does not read a moving or unavailable source", async () => {
  const dependencies = {
    fetchImpl: source(),
    publishFile: publisher().mockRejectedValueOnce(new Error("lost ack")),
  };
  await expect(runD1Capture(options, dependencies)).rejects.toThrow("lost ack");
  dependencies.fetchImpl.mockClear();
  expect((await runD1Capture({ ...options, batches: 1 }, dependencies)).afterSequence).toBe("1");
  expect(dependencies.fetchImpl).not.toHaveBeenCalled();
  expect(files.has(`${folder}/writer.lock`)).toBe(false);
});

test("rejects changed pending bytes and does not publish them", async () => {
  const publish = publisher().mockRejectedValueOnce(new Error("lost ack"));
  await expect(
    runD1Capture(options, { fetchImpl: source(), publishFile: publish }),
  ).rejects.toThrow("lost ack");
  const path = publish.mock.calls[0]?.[0];
  if (path === undefined) throw new Error("Expected artifact path");
  files.set(path, "corrupt");
  await expect(
    runD1Capture(options, { fetchImpl: source(), publishFile: publish }),
  ).rejects.toThrow("Pending capture bytes changed");
  expect(publish).toHaveBeenCalledTimes(1);
});

test("existing orphan artifacts must match immutable bytes", async () => {
  const dependencies = { fetchImpl: source(), publishFile: publisher() };
  await runD1Capture(options, dependencies);
  files.delete(`${folder}/checkpoint.json`);
  await runD1Capture(options, dependencies);
  files.delete(`${folder}/checkpoint.json`);
  const path = dependencies.publishFile.mock.calls[0]?.[0];
  if (path === undefined) throw new Error("Expected artifact path");
  files.set(path, "corrupt");
  await expect(runD1Capture(options, dependencies)).rejects.toThrow("artifact collision");
});

test.each([
  { databaseName: "other" },
  { accountId: "" },
  { token: "" },
  { directory: "" },
  { plans: [] },
  { batches: 0 },
  { batches: 1001 },
  { batches: 1.5 },
  { pageSize: 0 },
])("rejects invalid configuration before filesystem I/O", async (invalid) => {
  await expect(
    runD1Capture({ ...options, ...invalid }, { fetchImpl: source(), publishFile: publisher() }),
  ).rejects.toThrow();
  expect(fs.open).not.toHaveBeenCalled();
});

test("propagates state read errors and releases only its own lock", async () => {
  vi.mocked(fs.readFile).mockRejectedValueOnce(new Error("permission denied"));
  await expect(
    runD1Capture(options, { fetchImpl: source(), publishFile: publisher() }),
  ).rejects.toThrow("permission denied");
  expect(files.has(`${folder}/writer.lock`)).toBe(false);
});

test.each([
  null,
  { success: false },
  { success: true, result: null },
  { success: true, result: { name: "other" } },
])("rejects wrong ownership metadata", async (body) => {
  const fetchImpl = source().mockResolvedValueOnce(Response.json(body));
  await expect(runD1Capture(options, { fetchImpl, publishFile: publisher() })).rejects.toThrow(
    "ownership",
  );
});

test("rejects HTTP failure, missing DDL and changed source schema", async () => {
  await expect(
    runD1Capture(options, {
      fetchImpl: source().mockResolvedValueOnce(new Response("{}", { status: 503 })),
      publishFile: publisher(),
    }),
  ).rejects.toThrow("ownership");
  vi.mocked(install.prepareD1CaptureInstall).mockResolvedValueOnce({
    definitionHash: "0".repeat(64),
    ddlVerified: false,
    statements: [],
    plan: { createOutbox: "", triggers: [], schemaHash: "0".repeat(64), planHash: "0".repeat(64) },
  });
  await expect(
    runD1Capture(options, { fetchImpl: source(), publishFile: publisher() }),
  ).rejects.toThrow("DDL");
  vi.mocked(install.prepareD1CaptureInstall).mockResolvedValueOnce({
    definitionHash: "0".repeat(64),
    ddlVerified: true,
    statements: [],
    plan: { createOutbox: "", triggers: [], schemaHash: "1".repeat(64), planHash: "0".repeat(64) },
  });
  await expect(
    runD1Capture(options, { fetchImpl: source(), publishFile: publisher() }),
  ).rejects.toThrow("DDL");
});

test("invalid range discovery cannot publish", async () => {
  const fetchImpl = source()
    .mockResolvedValueOnce(Response.json({ success: true, result: { name: "venue-weather-db" } }))
    .mockResolvedValueOnce(result([]));
  await expect(runD1Capture(options, { fetchImpl, publishFile: publisher() })).rejects.toThrow(
    "boundary",
  );
});

test.each([
  { args: [] },
  { args: ["runner", "x", "--other", "/config"] },
  { args: ["runner", "x", "--config", ""] },
])("CLI requires exactly its config argument", async ({ args }) => {
  process.argv = args;
  await expect(runD1CaptureCli()).rejects.toThrow();
});

test.each([
  null,
  { plans: null },
  { plans: [], batches: "1", pageSize: 1000 },
  { plans: [], batches: 1, pageSize: "1000" },
])("CLI rejects malformed configuration", async (config) => {
  process.argv = ["runner", "x", "--config", "/config"];
  files.set("/config", JSON.stringify(config));
  await expect(runD1CaptureCli()).rejects.toThrow("configuration file");
});

test.each([0, 1])(
  "CLI checks publisher exit status and emits an unpromoted summary",
  async (exitCode) => {
    vi.stubEnv("R2_ACCOUNT_ID", "account");
    vi.stubEnv("CLOUDFLARE_DEBUG_TOKEN", "example");
    process.argv = ["runner", "x", "--config", "/config"];
    files.set("/config", JSON.stringify(options));
    vi.stubGlobal("fetch", source());
    vi.stubGlobal("Bun", {
      spawn: vi.fn((command: readonly string[]) => ({
        stdout: new Response(
          JSON.stringify({
            batchId: command[4]?.split("/").at(-1)?.replace(".json", ""),
            eventCount: 1,
            lastSequence: "1",
            reconciled: true,
          }),
        ).body,
        exited: Promise.resolve(exitCode),
      })),
    });
    const log = vi.spyOn(console, "log").mockImplementation(() => {});
    if (exitCode === 0) {
      await runD1CaptureCli();
      expect(log).toHaveBeenCalledWith(
        JSON.stringify({
          databaseName: "venue-weather-db",
          afterSequence: "1",
          pending: false,
          promoted: false,
        }),
      );
    } else await expect(runD1CaptureCli()).rejects.toThrow("publication failed");
  },
);

test("CLI validates plan objects and required plan fields", async () => {
  vi.stubEnv("R2_ACCOUNT_ID", "account");
  vi.stubEnv("CLOUDFLARE_DEBUG_TOKEN", "example");
  process.argv = ["runner", "x", "--config", "/config"];
  files.set("/config", JSON.stringify({ ...options, plans: [null] }));
  await expect(runD1CaptureCli()).rejects.toThrow("plan configuration");
  files.set("/config", JSON.stringify({ ...options, plans: [{}] }));
  await expect(runD1CaptureCli()).rejects.toThrow("Missing");
});
