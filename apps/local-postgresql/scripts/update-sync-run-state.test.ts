import { expect, it, vi } from "vitest";
import { createUpdateSyncRunStateStore, parseUpdateSyncCheckpoint } from "./update-sync-run-state";

it("parses a valid day-scoped checkpoint", () => {
  expect(
    parseUpdateSyncCheckpoint(
      '{"version":1,"runYmd":"20260825","nextStep":"sync","updatedAt":"2026-08-25T09:00:00.000Z"}',
    ),
  ).toStrictEqual({
    nextStep: "sync",
    runYmd: "20260825",
    updatedAt: "2026-08-25T09:00:00.000Z",
    version: 1,
  });
});

it("rejects malformed or unsupported checkpoints", () => {
  expect(() => parseUpdateSyncCheckpoint("[]")).toThrow("checkpoint is invalid");
  expect(() =>
    parseUpdateSyncCheckpoint(
      '{"version":2,"runYmd":"20260825","nextStep":"sync","updatedAt":"bad"}',
    ),
  ).toThrow("checkpoint is invalid");
  expect(() =>
    parseUpdateSyncCheckpoint(
      '{"version":1,"runYmd":"2026-08-25","nextStep":"unknown","updatedAt":"2026-08-25T09:00:00.000Z"}',
    ),
  ).toThrow("checkpoint is invalid");
});

it("writes checkpoint and completion records atomically and clears only the active checkpoint", async () => {
  const readText = vi.fn<(path: string) => Promise<string | null>>().mockResolvedValue(null);
  const remove = vi.fn<(path: string) => Promise<void>>().mockResolvedValue(undefined);
  const rename = vi
    .fn<(fromPath: string, toPath: string) => Promise<void>>()
    .mockResolvedValue(undefined);
  const writeText = vi
    .fn<(path: string, contents: string) => Promise<void>>()
    .mockResolvedValue(undefined);
  const store = createUpdateSyncRunStateStore({
    appDir: "/repo/apps/local-postgresql",
    io: { readText, remove, rename, writeText },
    randomId: () => "fixed-id",
  });

  await expect(store.loadCheckpoint()).resolves.toBeNull();
  await store.saveCheckpoint({
    nextStep: "training",
    runYmd: "20260825",
    updatedAt: "2026-08-25T09:00:00.000Z",
    version: 1,
  });
  await store.recordCompletion({
    completedAt: "2026-08-25T10:00:00.000Z",
    runYmd: "20260825",
    version: 1,
  });
  await store.clearCheckpoint();

  expect(writeText.mock.calls).toStrictEqual([
    [
      "/repo/apps/local-postgresql/tmp/update-and-sync-state/active.json.fixed-id.tmp",
      '{\n  "nextStep": "training",\n  "runYmd": "20260825",\n  "updatedAt": "2026-08-25T09:00:00.000Z",\n  "version": 1\n}\n',
    ],
    [
      "/repo/apps/local-postgresql/tmp/update-and-sync-state/completed-20260825.json.fixed-id.tmp",
      '{\n  "completedAt": "2026-08-25T10:00:00.000Z",\n  "runYmd": "20260825",\n  "version": 1\n}\n',
    ],
  ]);
  expect(rename.mock.calls).toStrictEqual([
    [
      "/repo/apps/local-postgresql/tmp/update-and-sync-state/active.json.fixed-id.tmp",
      "/repo/apps/local-postgresql/tmp/update-and-sync-state/active.json",
    ],
    [
      "/repo/apps/local-postgresql/tmp/update-and-sync-state/completed-20260825.json.fixed-id.tmp",
      "/repo/apps/local-postgresql/tmp/update-and-sync-state/completed-20260825.json",
    ],
  ]);
  expect(remove).toHaveBeenCalledWith(
    "/repo/apps/local-postgresql/tmp/update-and-sync-state/active.json",
  );
});

it("loads and validates persisted checkpoint contents", async () => {
  const store = createUpdateSyncRunStateStore({
    appDir: "/repo/apps/local-postgresql",
    io: {
      readText: vi
        .fn()
        .mockResolvedValue(
          '{"version":1,"runYmd":"20260825","nextStep":"readiness","updatedAt":"2026-08-25T09:00:00.000Z"}',
        ),
      remove: vi.fn().mockResolvedValue(undefined),
      rename: vi.fn().mockResolvedValue(undefined),
      writeText: vi.fn().mockResolvedValue(undefined),
    },
  });

  await expect(store.loadCheckpoint()).resolves.toStrictEqual({
    nextStep: "readiness",
    runYmd: "20260825",
    updatedAt: "2026-08-25T09:00:00.000Z",
    version: 1,
  });
});
