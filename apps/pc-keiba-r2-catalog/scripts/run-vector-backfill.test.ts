// Runs with bun via Vitest; CLI execution uses only mocked files and networks.
import { afterEach, beforeEach, expect, test, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  read: vi.fn(),
  write: vi.fn(),
  rename: vi.fn(),
  query: vi.fn(),
  fetch: vi.fn(),
}));
vi.mock("node:fs/promises", () => ({
  readFile: mocks.read,
  writeFile: mocks.write,
  rename: mocks.rename,
}));
vi.mock("@neondatabase/serverless", () => ({ neon: () => ({ query: mocks.query }) }));
const args: string[] = [
  "bun",
  "cli",
  "--namespace",
  "corner-v1",
  "--checkpoint",
  "/checkpoint.json",
  "--batches",
  "2",
  "--batch-size",
  "1",
  "--token-file",
  "/token",
  "--endpoint",
  "https://pc-keiba-r2-catalog.kaoru.workers.dev/v1/internal/vectors/upsert",
];
const row: Record<string, unknown> = {
  source: "jra",
  kaisai_nen: "2026",
  kaisai_tsukihi: "0913",
  keibajo_code: "06",
  race_bango: "01",
  ketto_toroku_bango: "2023100001",
  race_date: "20260913",
  feature_vector: "[0,0,0,0,0,0,0,0]",
};

beforeEach(() => {
  vi.resetModules();
  vi.clearAllMocks();
  mocks.read.mockReset().mockImplementation(async (path: string) => {
    if (path === "/token") return "token";
    throw Object.assign(new Error("Missing checkpoint"), { code: "ENOENT" });
  });
  mocks.write.mockReset().mockResolvedValue(undefined);
  mocks.rename.mockReset().mockResolvedValue(undefined);
  mocks.query.mockReset().mockResolvedValueOnce([row]).mockResolvedValue([]);
  mocks.fetch.mockReset().mockResolvedValue(Response.json({ mutationId: "m1" }, { status: 202 }));
  vi.stubGlobal("fetch", mocks.fetch);
  vi.stubGlobal("Bun", { argv: args });
  vi.stubEnv("NEON_PRIMARY_URL", "postgresql://test.invalid/source");
  vi.spyOn(console, "log").mockImplementation(() => undefined);
  vi.spyOn(console, "error").mockImplementation(() => undefined);
});

afterEach(() => {
  process.exitCode = 0;
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

test("submits bounded data and atomically checkpoints accepted progress", async () => {
  await import("./run-vector-backfill");
  expect(mocks.fetch).toHaveBeenCalledTimes(1);
  expect(mocks.query).toHaveBeenCalledTimes(2);
  expect(mocks.rename).toHaveBeenCalledWith("/checkpoint.json.next", "/checkpoint.json");
  expect(console.error).not.toHaveBeenCalled();
});

test("stops at the requested batch limit", async () => {
  vi.stubGlobal("Bun", { argv: args.map((value) => (value === "2" ? "1" : value)) });
  await import("./run-vector-backfill");
  expect(mocks.query).toHaveBeenCalledTimes(1);
});

test("resumes an already submitted checkpoint without rewriting data", async () => {
  mocks.read.mockImplementation(async (path: string) =>
    path === "/token"
      ? "token"
      : JSON.stringify({
          namespace: "corner-v1",
          cursor: {
            source: "jra",
            year: "2026",
            monthDay: "0913",
            venue: "06",
            race: "01",
            horse: "1",
          },
          submittedRows: 1,
          lastMutationId: "m1",
          phase: "submitted",
        }),
  );
  await import("./run-vector-backfill");
  expect(mocks.query).not.toHaveBeenCalled();
});

test.each([
  { from: "corner-v1", to: "invalid namespace" },
  { from: "2", to: "0" },
  { from: "2", to: "NaN" },
  {
    from: "https://pc-keiba-r2-catalog.kaoru.workers.dev/v1/internal/vectors/upsert",
    to: "http://pc-keiba-r2-catalog.kaoru.workers.dev",
  },
  {
    from: "https://pc-keiba-r2-catalog.kaoru.workers.dev/v1/internal/vectors/upsert",
    to: "https://other.invalid",
  },
  { from: "corner-v1", to: "" },
])("rejects unsafe CLI arguments before network I/O: %j", async ({ from, to }) => {
  vi.stubGlobal("Bun", { argv: args.map((value) => (value === from ? to : value)) });
  await import("./run-vector-backfill");
  expect(process.exitCode).toBe(1);
  expect(mocks.query).not.toHaveBeenCalled();
});

test("rejects missing source credentials", async () => {
  vi.stubEnv("NEON_PRIMARY_URL", "");
  await import("./run-vector-backfill");
  expect(process.exitCode).toBe(1);
  expect(mocks.query).not.toHaveBeenCalled();
});

test("rejects an empty authentication file", async () => {
  mocks.read.mockResolvedValue(" ");
  await import("./run-vector-backfill");
  expect(process.exitCode).toBe(1);
  expect(mocks.query).not.toHaveBeenCalled();
});

test.each([null, "invalid"])("does not reset a corrupt checkpoint", async (value) => {
  mocks.read.mockImplementation(async (path: string) =>
    path === "/token" ? "token" : JSON.stringify(value),
  );
  await import("./run-vector-backfill");
  expect(process.exitCode).toBe(1);
  expect(mocks.query).not.toHaveBeenCalled();
});

test("does not reset a checkpoint on a non-ENOENT read failure", async () => {
  mocks.read.mockResolvedValueOnce("token").mockRejectedValueOnce(null);
  await import("./run-vector-backfill");
  expect(process.exitCode).toBe(1);
});

test.each([null, [null]])("rejects malformed source responses", async (rows) => {
  mocks.query.mockReset().mockResolvedValue(rows);
  await import("./run-vector-backfill");
  expect(process.exitCode).toBe(1);
  expect(mocks.fetch).not.toHaveBeenCalled();
});

test("does not advance on an HTTP failure", async () => {
  mocks.fetch.mockResolvedValue(new Response(null, { status: 503 }));
  await import("./run-vector-backfill");
  expect(process.exitCode).toBe(1);
  expect(mocks.write).not.toHaveBeenCalled();
});

test.each([null, { mutationId: 1 }])(
  "does not advance on malformed acceptance receipts",
  async (receipt) => {
    mocks.fetch.mockResolvedValue(Response.json(receipt, { status: 202 }));
    await import("./run-vector-backfill");
    expect(process.exitCode).toBe(1);
    expect(mocks.write).not.toHaveBeenCalled();
  },
);
