// This file runs with Bun and Vitest; all I/O is mocked.
import { beforeEach, afterEach, expect, test, vi } from "vitest";
import layouts from "../../daily-keiba-sync/src/generated/record-layouts.json";
import { buildProductionRequest, type ProductionRequestInput } from "./production-request";
import {
  createProductionCliRuntime,
  parseOperatorResponse,
  runProductionCli,
  type ProductionCliRuntime,
} from "./production-cli";

const io = vi.hoisted(() => ({
  read: vi.fn(),
  write: vi.fn(),
  mkdir: vi.fn(),
  access: vi.fn(),
  connect: vi.fn(),
  execute: vi.fn(),
  end: vi.fn(),
}));
vi.mock("node:fs/promises", () => ({
  readFile: io.read,
  writeFile: io.write,
  mkdir: io.mkdir,
  access: io.access,
}));
vi.mock("./storage/pg-client", () => ({
  resolvePostgresConfig: vi.fn(),
  createPostgresClient: () => ({ connect: io.connect, execute: io.execute, end: io.end }),
}));
const REQUEST: ProductionRequestInput = {
  runId: "12345678-abcd-4321-9876-123456789abc",
  createdAt: "2026-09-18T18:00:00Z",
  race: {
    ...Object.fromEntries(layouts.tables.jvd_ra.columns.map((c) => [c.name, ""])),
    kaisai_nen: "2026",
    kaisai_tsukihi: "0919",
    keibajo_code: "A4",
    race_bango: "05",
    shusso_tosu: "01",
  },
  runners: [
    {
      ...Object.fromEntries(layouts.tables.jvd_se.columns.map((c) => [c.name, ""])),
      kaisai_nen: "2026",
      kaisai_tsukihi: "0919",
      keibajo_code: "A4",
      race_bango: "05",
      umaban: "07",
      ketto_toroku_bango: "2021105727",
    },
  ],
};
const ENV: Readonly<Record<string, string>> = {
  PC_KEIBA_CLOUDFLARE_PROFILE: "default",
  CLOUDFLARE_ACCOUNT_ID: "00000000000000000000000000000001",
  PC_KEIBA_SYNC_DATABASE_ID: "database-id",
  PC_KEIBA_CATALOG_QUEUE_ID: "queue-id",
  PC_KEIBA_SOURCE_STAGING_BUCKET: "source-bucket",
};
const RESPONSE: string = JSON.stringify({
  ok: true,
  data: { content: [{ type: "text", text: JSON.stringify({ success: true, result: [] }) }] },
});
const RUNTIME: ProductionCliRuntime = {
  read: vi.fn(),
  write: vi.fn().mockResolvedValue(undefined),
  exists: vi.fn().mockResolvedValue(false),
  command: vi.fn().mockResolvedValue(RESPONSE),
  loadRace: vi.fn().mockResolvedValue(REQUEST),
};
beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(RUNTIME.read).mockReset();
  vi.mocked(RUNTIME.exists).mockResolvedValue(false);
  vi.mocked(RUNTIME.command).mockResolvedValue(RESPONSE);
  io.execute.mockReset();
  io.access.mockReset();
});
afterEach(() => vi.unstubAllGlobals());

test("prepares an immutable local request without production commands", async () => {
  const result: unknown = await runProductionCli({
    argv: ["prepare", "/cache", "2026", "0919", "A4", "05"],
    env: {},
    runtime: RUNTIME,
  });
  expect(result).toStrictEqual({
    prepared: true,
    runId: "12345678-abcd-4321-9876-123456789abc",
    productionWrites: 0,
  });
  expect(RUNTIME.write).toHaveBeenCalledTimes(2);
  expect(RUNTIME.command).not.toHaveBeenCalled();
  expect(RUNTIME.loadRace).toHaveBeenCalledWith(["2026", "0919", "A4", "05"]);
});
test("will not replace an existing request", async () => {
  vi.mocked(RUNTIME.exists).mockResolvedValue(true);
  await expect(
    runProductionCli({
      argv: ["prepare", "/cache", "2026", "0919", "A4", "05"],
      env: {},
      runtime: RUNTIME,
    }),
  ).rejects.toThrow("Durable request already exists");
  expect(RUNTIME.loadRace).not.toHaveBeenCalled();
});
test("requires a directory", async () => {
  await expect(runProductionCli({ argv: [], env: {}, runtime: RUNTIME })).rejects.toThrow(
    "Use prepare",
  );
});
test("rejects unknown commands", async () => {
  await expect(
    runProductionCli({ argv: ["other", "/cache"], env: {}, runtime: RUNTIME }),
  ).rejects.toThrow("Invalid production command");
});
test("requires explicit production confirmation", async () => {
  await expect(
    runProductionCli({ argv: ["apply", "/cache"], env: ENV, runtime: RUNTIME }),
  ).rejects.toThrow("missing --confirm-production");
  expect(RUNTIME.command).not.toHaveBeenCalled();
});
test("rejects incorrect confirmation", async () => {
  await expect(
    runProductionCli({ argv: ["apply", "/cache", "yes"], env: ENV, runtime: RUNTIME }),
  ).rejects.toThrow("missing --confirm-production");
});
test("rejects extra status arguments", async () => {
  await expect(
    runProductionCli({ argv: ["status", "/cache", "extra"], env: ENV, runtime: RUNTIME }),
  ).rejects.toThrow("Invalid production command");
});
test("rejects incomplete prepare arguments", async () => {
  await expect(
    runProductionCli({ argv: ["prepare", "/cache"], env: ENV, runtime: RUNTIME }),
  ).rejects.toThrow("Invalid production command");
});
test("reads status without staging or registration", async () => {
  vi.mocked(RUNTIME.read).mockResolvedValue(JSON.stringify(REQUEST));
  expect(
    await runProductionCli({ argv: ["status", "/cache"], env: ENV, runtime: RUNTIME }),
  ).toStrictEqual([]);
  expect(RUNTIME.command).toHaveBeenCalledTimes(1);
  expect(RUNTIME.write).not.toHaveBeenCalled();
});
test("rejects missing settings before any remote operation", async () => {
  vi.mocked(RUNTIME.read).mockResolvedValue(JSON.stringify(REQUEST));
  await expect(
    runProductionCli({ argv: ["status", "/cache"], env: {}, runtime: RUNTIME }),
  ).rejects.toThrow("Missing or invalid operator setting");
  expect(RUNTIME.command).not.toHaveBeenCalled();
});
test("rejects unsafe resource paths", async () => {
  vi.mocked(RUNTIME.read).mockResolvedValue(JSON.stringify(REQUEST));
  await expect(
    runProductionCli({
      argv: ["status", "/cache"],
      env: { ...ENV, PC_KEIBA_SYNC_DATABASE_ID: "../other" },
      runtime: RUNTIME,
    }),
  ).rejects.toThrow("Missing or invalid operator setting");
});
test("rejects missing API profiles", async () => {
  vi.mocked(RUNTIME.read).mockResolvedValue(JSON.stringify(REQUEST));
  await expect(
    runProductionCli({
      argv: ["status", "/cache"],
      env: { ...ENV, PC_KEIBA_CLOUDFLARE_PROFILE: "" },
      runtime: RUNTIME,
    }),
  ).rejects.toThrow("Missing or invalid operator setting");
});
test("rejects a null manifest", async () => {
  vi.mocked(RUNTIME.read).mockResolvedValue("null");
  await expect(
    runProductionCli({ argv: ["status", "/cache"], env: ENV, runtime: RUNTIME }),
  ).rejects.toThrow("Invalid production input manifest");
});
test("rejects an array manifest", async () => {
  vi.mocked(RUNTIME.read).mockResolvedValue("[]");
  await expect(
    runProductionCli({ argv: ["status", "/cache"], env: ENV, runtime: RUNTIME }),
  ).rejects.toThrow("Invalid production input manifest");
});
test("rejects missing run IDs", async () => {
  vi.mocked(RUNTIME.read).mockResolvedValue(JSON.stringify({ ...REQUEST, runId: 1 }));
  await expect(
    runProductionCli({ argv: ["status", "/cache"], env: ENV, runtime: RUNTIME }),
  ).rejects.toThrow("Invalid production input manifest");
});
test("rejects missing creation times", async () => {
  vi.mocked(RUNTIME.read).mockResolvedValue(JSON.stringify({ ...REQUEST, createdAt: null }));
  await expect(
    runProductionCli({ argv: ["status", "/cache"], env: ENV, runtime: RUNTIME }),
  ).rejects.toThrow("Invalid production input manifest");
});
test("rejects non-record race rows", async () => {
  vi.mocked(RUNTIME.read).mockResolvedValue(JSON.stringify({ ...REQUEST, race: 1 }));
  await expect(
    runProductionCli({ argv: ["status", "/cache"], env: ENV, runtime: RUNTIME }),
  ).rejects.toThrow("Invalid production input manifest");
});
test("rejects non-string source cells", async () => {
  vi.mocked(RUNTIME.read).mockResolvedValue(
    JSON.stringify({ ...REQUEST, race: { ...REQUEST.race, shusso_tosu: 1 } }),
  );
  await expect(
    runProductionCli({ argv: ["status", "/cache"], env: ENV, runtime: RUNTIME }),
  ).rejects.toThrow("Invalid production input manifest");
});
test("rejects non-array runners", async () => {
  vi.mocked(RUNTIME.read).mockResolvedValue(JSON.stringify({ ...REQUEST, runners: null }));
  await expect(
    runProductionCli({ argv: ["status", "/cache"], env: ENV, runtime: RUNTIME }),
  ).rejects.toThrow("Invalid production input manifest");
});
test("rejects malformed runner rows", async () => {
  vi.mocked(RUNTIME.read).mockResolvedValue(JSON.stringify({ ...REQUEST, runners: [null] }));
  await expect(
    runProductionCli({ argv: ["status", "/cache"], env: ENV, runtime: RUNTIME }),
  ).rejects.toThrow("Invalid production input manifest");
});
test("accepts nullable non-key source cells", async () => {
  vi.mocked(RUNTIME.read).mockResolvedValue(
    JSON.stringify({ ...REQUEST, race: { ...REQUEST.race, data_kubun: null } }),
  );
  expect(
    await runProductionCli({ argv: ["status", "/cache"], env: ENV, runtime: RUNTIME }),
  ).toStrictEqual([]);
});
test("does not re-enqueue accepted requests", async () => {
  vi.mocked(RUNTIME.exists).mockResolvedValue(true);
  await expect(
    runProductionCli({
      argv: ["apply", "/cache", "--confirm-production"],
      env: ENV,
      runtime: RUNTIME,
    }),
  ).rejects.toThrow("Request already accepted");
  expect(RUNTIME.command).not.toHaveBeenCalled();
});
test("rejects changed durable plans", async () => {
  vi.mocked(RUNTIME.read)
    .mockResolvedValueOnce(JSON.stringify(REQUEST))
    .mockResolvedValueOnce("{}");
  await expect(
    runProductionCli({
      argv: ["apply", "/cache", "--confirm-production"],
      env: ENV,
      runtime: RUNTIME,
    }),
  ).rejects.toThrow("Durable request changed");
  expect(RUNTIME.command).not.toHaveBeenCalled();
});
test("stops before registration when staging readback differs", async () => {
  vi.mocked(RUNTIME.read)
    .mockResolvedValueOnce(JSON.stringify(REQUEST))
    .mockResolvedValueOnce(JSON.stringify(buildProductionRequest(REQUEST)))
    .mockResolvedValue("changed");
  await expect(
    runProductionCli({
      argv: ["apply", "/cache", "--confirm-production"],
      env: ENV,
      runtime: RUNTIME,
    }),
  ).rejects.toThrow("Remote staging digest mismatch");
  expect(RUNTIME.command).toHaveBeenCalledTimes(5);
  expect(vi.mocked(RUNTIME.command).mock.calls.map(([args]) => args[0])).toStrictEqual([
    "executor",
    "bunx",
    "bunx",
    "bunx",
    "bunx",
  ]);
});
test("uploads and verifies both stages before registration and queue submission", async () => {
  const plan = buildProductionRequest(REQUEST);
  vi.mocked(RUNTIME.read)
    .mockResolvedValueOnce(JSON.stringify(REQUEST))
    .mockResolvedValueOnce(JSON.stringify(plan))
    .mockResolvedValueOnce(plan.stages[0]?.content ?? "")
    .mockResolvedValueOnce(plan.stages[1]?.content ?? "");
  expect(
    await runProductionCli({
      argv: ["apply", "/cache", "--confirm-production"],
      env: ENV,
      runtime: RUNTIME,
    }),
  ).toStrictEqual({
    accepted: true,
    runId: "12345678-abcd-4321-9876-123456789abc",
    published: false,
  });
  expect(RUNTIME.command).toHaveBeenCalledTimes(11);
  expect(RUNTIME.write).toHaveBeenLastCalledWith(
    "/cache/production-accepted.json",
    '{"runId":"12345678-abcd-4321-9876-123456789abc","accepted":true}',
  );
});
test("rejects non-object API envelopes", () => {
  expect(() => parseOperatorResponse("null")).toThrow("Managed API execution failed");
});
test("rejects unsuccessful API envelopes", () => {
  expect(() => parseOperatorResponse('{"ok":false}')).toThrow("Managed API execution failed");
});
test("rejects missing API data", () => {
  expect(() => parseOperatorResponse('{"ok":true,"data":null}')).toThrow(
    "Managed API execution failed",
  );
});
test("rejects tool errors", () => {
  expect(() => parseOperatorResponse('{"ok":true,"data":{"isError":true}}')).toThrow(
    "Managed API execution failed",
  );
});
test("rejects missing content", () => {
  expect(() => parseOperatorResponse('{"ok":true,"data":{}}')).toThrow(
    "Managed API execution failed",
  );
});
test("rejects missing text responses", () => {
  expect(() =>
    parseOperatorResponse('{"ok":true,"data":{"content":[null,{"type":"image"}]}}'),
  ).toThrow("Managed API response is missing");
});
test("rejects non-string text responses", () => {
  expect(() =>
    parseOperatorResponse('{"ok":true,"data":{"content":[{"type":"text","text":1}]}}'),
  ).toThrow("Managed API response is missing");
});
test("rejects unsuccessful API results", () => {
  expect(() =>
    parseOperatorResponse(
      JSON.stringify({
        ok: true,
        data: { content: [{ type: "text", text: '{"success":false}' }] },
      }),
    ),
  ).toThrow("Production API operation failed");
});
test("rejects non-object API results", () => {
  expect(() =>
    parseOperatorResponse(
      JSON.stringify({ ok: true, data: { content: [{ type: "text", text: "null" }] } }),
    ),
  ).toThrow("Production API operation failed");
});
test("native runtime reads private files", async () => {
  io.read.mockResolvedValue("data");
  expect(await createProductionCliRuntime().read("/cache/input")).toBe("data");
  expect(io.read).toHaveBeenCalledWith("/cache/input", "utf8");
});
test("native runtime writes owner-only artifacts", async () => {
  await createProductionCliRuntime().write("/cache/input", "data");
  expect(io.mkdir).toHaveBeenCalledWith("/cache", { recursive: true, mode: 448 });
  expect(io.write).toHaveBeenCalledWith("/cache/input", "data", { mode: 384 });
});
test("native runtime detects existing files", async () => {
  io.access.mockResolvedValue(undefined);
  expect(await createProductionCliRuntime().exists("/cache/input")).toBe(true);
});
test("native runtime detects missing files", async () => {
  io.access.mockRejectedValue(new Error("missing"));
  expect(await createProductionCliRuntime().exists("/cache/input")).toBe(false);
});
test("native runtime drains subprocess output", async () => {
  const spawn = vi.fn().mockReturnValue({
    stdout: new Response("ok").body,
    stderr: new Response("").body,
    exited: Promise.resolve(0),
  });
  vi.stubGlobal("Bun", { spawn });
  expect(await createProductionCliRuntime().command(["executor", "tool"])).toBe("ok");
  expect(spawn).toHaveBeenCalledWith(["executor", "tool"], { stdout: "pipe", stderr: "pipe" });
});
test("native runtime hides subprocess failures and secrets", async () => {
  vi.stubGlobal("Bun", {
    spawn: vi.fn().mockReturnValue({
      stdout: new Response("private").body,
      stderr: new Response("secret").body,
      exited: Promise.resolve(1),
    }),
  });
  await expect(createProductionCliRuntime().command(["executor"])).rejects.toThrow(
    "Operator command failed",
  );
});
test("native local preparation closes its database client", async () => {
  io.execute
    .mockResolvedValueOnce({ rows: [REQUEST.race] })
    .mockResolvedValueOnce({ rows: REQUEST.runners });
  const input: ProductionRequestInput = await createProductionCliRuntime().loadRace([
    "2026",
    "0919",
    "A4",
    "05",
  ]);
  expect(input.runners).toHaveLength(1);
  expect(io.execute).toHaveBeenCalledTimes(2);
  expect(io.end).toHaveBeenCalledTimes(1);
});
test("native local preparation rejects missing races and closes its client", async () => {
  io.execute.mockResolvedValue({ rows: [] });
  await expect(createProductionCliRuntime().loadRace([])).rejects.toThrow(
    "Expected exactly one local race",
  );
  expect(io.end).toHaveBeenCalledTimes(1);
});
test("native local preparation rejects ambiguous races", async () => {
  io.execute
    .mockResolvedValueOnce({ rows: [REQUEST.race, REQUEST.race] })
    .mockResolvedValueOnce({ rows: [] });
  await expect(createProductionCliRuntime().loadRace([])).rejects.toThrow(
    "Expected exactly one local race",
  );
  expect(io.end).toHaveBeenCalledTimes(1);
});
