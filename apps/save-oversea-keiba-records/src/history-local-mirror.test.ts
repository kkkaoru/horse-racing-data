// This file runs with Bun. Database, filesystem and source preparation are mocked.
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import {
  localHistoryEnvironment,
  runHistoryLocalMirror,
  runHistoryProductionMirror,
} from "./history-local-mirror";
import type { SecondaryPersonResult } from "./sources/secondary-result-parser";
import type { HistoryPublicationInput } from "./storage/history-publication";
const ports = vi.hoisted(() => ({
  readFile: vi.fn(),
  mkdir: vi.fn(),
  writeFile: vi.fn(),
  connect: vi.fn(),
  close: vi.fn(),
  apply: vi.fn(),
  prepare: vi.fn(),
  source: vi.fn(),
  conflicts: vi.fn(),
}));
vi.mock("node:fs/promises", () => ({
  readFile: ports.readFile,
  mkdir: ports.mkdir,
  writeFile: ports.writeFile,
}));
vi.mock("./storage/history-connection", () => ({ createHistoryConnection: ports.connect }));
vi.mock("./history-json", () => ({
  decodeHistoryPlan: JSON.parse,
  decodeHistoryArchive: JSON.parse,
}));
vi.mock("./history-preparation", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./history-preparation")>()),
  prepareHistoryArchive: ports.source,
}));
const row: SecondaryPersonResult = {
  personKind: "owner",
  sourcePersonId: "demo",
  sourceRaceId: "race1",
  raceDate: "2026-09-01",
  venue: "Venue",
  raceNumber: "1",
  raceName: "Race",
  sourceRaceUrl: "https://example.test/event/race1",
  sourceHorseId: "horse1",
  horseName: "Horse",
  finishPosition: 2,
  finishPositionText: "2",
  surface: "Dirt",
  distanceMetres: 1800,
  going: "Good",
};
const rows: readonly SecondaryPersonResult[] = Array.from({ length: 251 }, (_, index) => ({
  ...row,
  sourceRaceId: `race${index}`,
}));
const env = {
  OVERSEA_HISTORY_LOCAL_DATABASE_URL: "postgresql://user:private@127.0.0.1:15432/racing",
};
const argv: readonly string[] = ["mirror-local", "/source", "/receipts/new", "--confirm-write"];
beforeEach(() => {
  vi.resetAllMocks();
  ports.readFile.mockImplementation(async (path: string) =>
    path.endsWith("plan.json") ? '{"plan":true}' : '{"archive":true}',
  );
  ports.mkdir.mockResolvedValue(undefined);
  ports.writeFile.mockResolvedValue(undefined);
  ports.close.mockResolvedValue(undefined);
  ports.source.mockReturnValue({
    input: { people: rows, horses: [] },
    archivedRows: 251,
    publishedCount: 500,
    sourceComplete: false,
    sourcePartialRows: [],
    duplicateRows: 0,
  });
  ports.connect.mockReturnValue({
    targetFingerprint: "a".repeat(64),
    database: { apply: ports.apply, prepare: ports.prepare, conflicts: ports.conflicts },
    close: ports.close,
  });
  ports.apply.mockImplementation(async (input: HistoryPublicationInput) => ({
    submittedRows: input.people.length,
    insertedRows: input.people.length,
    verifiedRows: input.people.length,
  }));
  ports.prepare.mockResolvedValue([]);
  ports.conflicts.mockResolvedValue([]);
});
afterEach(() => {
  vi.restoreAllMocks();
});
it("freezes the source and mirrors 250/1 chunks with post-commit verification", async () => {
  const result = await runHistoryLocalMirror({ argv, env });
  expect(result.receipt).toStrictEqual({
    submittedRows: 251,
    insertedRows: 251,
    verifiedRows: 251,
  });
  expect(result.sourceComplete).toBe(false);
  expect(result.canonicalCoverageComplete).toBe(false);
  expect(ports.apply.mock.calls.map((call) => call[0].people.length)).toStrictEqual([250, 1]);
  expect(ports.prepare).toHaveBeenCalledTimes(2);
  expect(ports.close).toHaveBeenCalledTimes(1);
  expect(ports.connect).toHaveBeenCalledWith(
    expect.objectContaining({
      OVERSEA_HISTORY_LOCAL_DATABASE_URL: "postgresql://user:private@127.0.0.1:15432/racing",
    }),
    "local",
  );
  expect(ports.writeFile).toHaveBeenLastCalledWith(
    "/receipts/new/receipt.json",
    expect.stringMatching(/"databasePublished":true/u),
    { flag: "wx", mode: 0o600 },
  );
});
it("requires explicit production confirmation and binds production receipts", async () => {
  const productionEnv = {
    OVERSEA_HISTORY_PRODUCTION_DATABASE_URL:
      "postgresql://user:private@production.test/racing?sslmode=verify-full",
  };
  const result = await runHistoryProductionMirror({
    argv: ["mirror-production", "/source", "/receipts/new", "--confirm-production"],
    env: productionEnv,
  });
  expect(ports.connect).toHaveBeenCalledWith(productionEnv, "production");
  expect(result.receipt?.verifiedRows).toBe(251);
  expect(ports.prepare).toHaveBeenCalledTimes(2);
  expect(ports.writeFile).toHaveBeenLastCalledWith(
    "/receipts/new/receipt.json",
    expect.stringMatching(/"target":"production"/u),
    { flag: "wx", mode: 0o600 },
  );
});
it("does not accept local confirmation for production or production action through the local entrypoint", async () => {
  await expect(
    runHistoryProductionMirror({
      argv: ["mirror-production", "/source", "/new", "--confirm-write"],
      env,
    }),
  ).rejects.toThrow("Use mirror-local");
  await expect(
    runHistoryLocalMirror({
      argv: ["mirror-production", "/source", "/new", "--confirm-production"],
      env,
    }),
  ).rejects.toThrow("Use mirror-local");
  expect(ports.connect).not.toHaveBeenCalled();
});
it("does not fall back to local credentials on a production connection error", async () => {
  ports.connect.mockImplementation(() => {
    throw new Error("An explicit private history database URL is required.");
  });
  await expect(
    runHistoryProductionMirror({
      argv: ["mirror-production", "/source", "/new", "--confirm-production"],
      env,
    }),
  ).rejects.toThrow("explicit private history database URL");
  expect(ports.connect).toHaveBeenCalledTimes(1);
  expect(ports.apply).not.toHaveBeenCalled();
});
it("preserves explicitly requested existing variants and still inserts other rows", async () => {
  ports.conflicts.mockResolvedValueOnce([rows[0]]);
  ports.source.mockReturnValue({
    input: { people: rows, horses: [] },
    archivedRows: 251,
    publishedCount: 251,
    sourceComplete: true,
    sourcePartialRows: [],
    duplicateRows: 0,
  });
  const result = await runHistoryLocalMirror({
    argv: [...argv, "--preserve-existing-variants"],
    env,
  });
  expect(result.existingConflictingRows).toBe(1);
  expect(result.receipt).toStrictEqual({
    submittedRows: 250,
    insertedRows: 250,
    verifiedRows: 250,
  });
  expect(result.canonicalCoverageComplete).toBe(false);
  expect(ports.apply.mock.calls.map((call) => call[0].people.length)).toStrictEqual([249, 1]);
  expect(ports.writeFile).toHaveBeenCalledWith(
    "/receipts/new/chunk-0-pending.json",
    expect.stringMatching(/"existingVariantsPreserved":\[\{/u),
    { flag: "wx", mode: 0o600 },
  );
});
it("rejects unknown conflict policies", async () => {
  await expect(runHistoryLocalMirror({ argv: [...argv, "--overwrite"], env })).rejects.toThrow(
    "Use mirror-local",
  );
});
it("uses current DB delta on subsequent passes instead of resubmitting old inserts", async () => {
  ports.apply.mockImplementation(async (input: HistoryPublicationInput) => ({
    submittedRows: 0,
    insertedRows: 0,
    verifiedRows: input.people.length,
  }));
  expect((await runHistoryLocalMirror({ argv, env })).receipt).toStrictEqual({
    submittedRows: 0,
    insertedRows: 0,
    verifiedRows: 251,
  });
});
it("reports a genuinely complete source without partial rows", async () => {
  ports.source.mockReturnValue({
    input: { people: [], horses: [] },
    archivedRows: 0,
    publishedCount: 0,
    sourceComplete: true,
    sourcePartialRows: [],
    duplicateRows: 0,
  });
  const result = await runHistoryLocalMirror({ argv, env });
  expect(result.canonicalCoverageComplete).toBe(true);
  expect(result.receipt).toStrictEqual({ submittedRows: 0, insertedRows: 0, verifiedRows: 0 });
  expect(ports.apply).not.toHaveBeenCalled();
});
it("keeps genuine source-partial rows outside canonical completeness", async () => {
  ports.source.mockReturnValue({
    input: { people: [], horses: [] },
    archivedRows: 1,
    publishedCount: 1,
    sourceComplete: true,
    sourcePartialRows: [row],
    duplicateRows: 0,
  });
  expect((await runHistoryLocalMirror({ argv, env })).canonicalCoverageComplete).toBe(false);
});
it.each([
  { args: [] },
  { args: ["other"] },
  { args: ["mirror-local", "/source", "/receipt"] },
  { args: ["mirror-local", "/source", "/receipt", "yes"] },
  { args: ["mirror-local", "", "/receipt", "--confirm-write"] },
  { args: ["mirror-local", "/source", "", "--confirm-write"] },
])("rejects invalid commands %j", async ({ args }) => {
  await expect(runHistoryLocalMirror({ argv: args, env })).rejects.toThrow("Use mirror-local");
  expect(ports.connect).not.toHaveBeenCalled();
});
it("does not write evidence into the live source directory", async () => {
  await expect(
    runHistoryLocalMirror({
      argv: ["mirror-local", "/source", "/source/.", "--confirm-write"],
      env,
    }),
  ).rejects.toThrow("live source directory");
});
it("rejects plan drift before reserving a destination", async () => {
  ports.readFile
    .mockResolvedValueOnce("{}")
    .mockResolvedValueOnce("{}")
    .mockResolvedValueOnce('{"changed":true}');
  await expect(runHistoryLocalMirror({ argv, env })).rejects.toThrow("Source plan changed");
  expect(ports.mkdir).not.toHaveBeenCalled();
});
it("refuses an existing evidence directory instead of replaying a pending job", async () => {
  ports.mkdir.mockRejectedValue({ code: "EEXIST" });
  await expect(runHistoryLocalMirror({ argv, env })).rejects.toStrictEqual({ code: "EEXIST" });
  expect(ports.connect).not.toHaveBeenCalled();
});
it("fails closed on conflicting or invalid source rows before DB writes", async () => {
  ports.source.mockImplementation(() => {
    throw new Error("conflicting records");
  });
  await expect(runHistoryLocalMirror({ argv, env })).rejects.toThrow("conflicting records");
  expect(ports.connect).not.toHaveBeenCalled();
});
it("retains pending evidence and closes the DB after transaction failure", async () => {
  ports.apply.mockRejectedValue(new Error("transaction rollback"));
  await expect(runHistoryLocalMirror({ argv, env })).rejects.toThrow("transaction rollback");
  expect(ports.close).toHaveBeenCalledTimes(1);
  expect(ports.writeFile).not.toHaveBeenCalledWith(
    "/receipts/new/receipt.json",
    expect.anything(),
    expect.anything(),
  );
});
it("rejects a wrong per-chunk verification count", async () => {
  ports.apply.mockResolvedValue({ submittedRows: 1, insertedRows: 1, verifiedRows: 1 });
  await expect(runHistoryLocalMirror({ argv, env })).rejects.toThrow(
    "Chunk verification count differs",
  );
  expect(ports.close).toHaveBeenCalledTimes(1);
});
it("rejects failed post-commit readback rather than claiming successful mirroring", async () => {
  ports.prepare.mockResolvedValue([row]);
  await expect(runHistoryLocalMirror({ argv, env })).rejects.toThrow(
    "post-commit readback differs",
  );
  expect(ports.close).toHaveBeenCalledTimes(1);
});
it("builds only loopback URLs from explicit PostgreSQL variables", () => {
  expect(
    localHistoryEnvironment({
      POSTGRES_PORT: "15432",
      POSTGRES_USER: "user",
      POSTGRES_PASSWORD: "a@b",
      POSTGRES_DB: "racing",
    }).OVERSEA_HISTORY_LOCAL_DATABASE_URL,
  ).toBe("postgresql://user:a%40b@127.0.0.1:15432/racing");
});
it("requires every explicit local setting", () => {
  expect(() => localHistoryEnvironment({})).toThrow("Explicit local PostgreSQL configuration");
});
it.each([
  "postgresql://user:pass@remote.example/db",
  "postgresql://user:pass@127.0.0.1/db?host=remote.example",
])("rejects remote endpoints and URL overrides %s", (value) => {
  expect(() => localHistoryEnvironment({ OVERSEA_HISTORY_LOCAL_DATABASE_URL: value })).toThrow();
});
it("accepts an explicit local TLS option", () => {
  expect(
    localHistoryEnvironment({
      OVERSEA_HISTORY_LOCAL_DATABASE_URL: "postgresql://user:pass@localhost/db?sslmode=disable",
    }).OVERSEA_HISTORY_LOCAL_DATABASE_URL,
  ).toBe("postgresql://user:pass@localhost/db?sslmode=disable");
});
