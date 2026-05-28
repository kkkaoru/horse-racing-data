// Run with bun.
import { expect, it, vi } from "vitest";

import {
  assertDeleteConfirmed,
  buildDefaultConfig,
  deleteOddsSnapshotsChunked,
  isWithinNightWindow,
  type DeleteChunkResponse,
  type DeleteOddsSnapshotsChunkConfig,
} from "./delete-odds-snapshots-chunk";

const buildConfig = (
  overrides: Partial<DeleteOddsSnapshotsChunkConfig> = {},
): DeleteOddsSnapshotsChunkConfig => ({
  adminToken: "admin",
  batchSize: 500,
  circuitPauseMs: 1,
  fetchImpl: vi.fn(),
  internalToken: "internal",
  newWorkerUrl: "https://new.example.com",
  nowImpl: () => new Date("2026-05-28T15:00:00Z"),
  oldWorkerUrl: "https://old.example.com",
  retryBackoffMs: 1,
  retryLimit: 3,
  sleepImpl: vi.fn(async () => undefined),
  sleepMs: 1,
  upperBoundId: 1000,
  ...overrides,
});

const buildResponse = (body: unknown, init?: ResponseInit): Response =>
  new Response(JSON.stringify(body), {
    headers: { "content-type": "application/json" },
    status: init?.status ?? 200,
  });

it("isWithinNightWindow returns true at JST 00:00", () => {
  expect(isWithinNightWindow(new Date("2026-05-28T15:00:00Z"))).toBe(true);
});

it("isWithinNightWindow returns true at JST 23:30", () => {
  expect(isWithinNightWindow(new Date("2026-05-28T14:30:00Z"))).toBe(true);
});

it("isWithinNightWindow returns false at JST 12:00", () => {
  expect(isWithinNightWindow(new Date("2026-05-28T03:00:00Z"))).toBe(false);
});

it("isWithinNightWindow returns false at JST 04:00", () => {
  expect(isWithinNightWindow(new Date("2026-05-28T19:00:00Z"))).toBe(false);
});

it("stops immediately when outside night window", async () => {
  const fetchImpl = vi.fn(async () => buildResponse({ value: "0" }));
  const config = buildConfig({
    fetchImpl,
    nowImpl: () => new Date("2026-05-28T03:00:00Z"),
  });
  const result = await deleteOddsSnapshotsChunked(config);
  expect(result.stoppedReason).toBe("outside-night-window");
  expect(result.totalDeleted).toBe(0);
});

it("stops when resume id already meets the upper bound", async () => {
  const fetchImpl = vi.fn(async () => buildResponse({ value: "2000" }));
  const config = buildConfig({ fetchImpl, upperBoundId: 1000 });
  const result = await deleteOddsSnapshotsChunked(config);
  expect(result.stoppedReason).toBe("upper-bound-reached");
});

it("deletes a chunk and saves progress when within bounds", async () => {
  const chunkOne: DeleteChunkResponse = { deleted: 500, done: false, next_since_id: 500 };
  const chunkTwo: DeleteChunkResponse = { deleted: 100, done: true, next_since_id: 600 };
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValueOnce(buildResponse(chunkOne))
    .mockResolvedValueOnce(buildResponse({ ok: true }))
    .mockResolvedValueOnce(buildResponse(chunkTwo))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ fetchImpl });
  const result = await deleteOddsSnapshotsChunked(config);
  expect(result.stoppedReason).toBe("completed");
  expect(result.totalDeleted).toBe(600);
});

it("treats non-ok resume response as zero", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ error: "x" }, { status: 404 }))
    .mockResolvedValueOnce(buildResponse({ deleted: 0, done: true, next_since_id: 0 }))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ fetchImpl });
  const result = await deleteOddsSnapshotsChunked(config);
  expect(result.totalDeleted).toBe(0);
});

it("treats non-numeric resume value as zero", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "garbage" }))
    .mockResolvedValueOnce(buildResponse({ deleted: 0, done: true, next_since_id: 0 }))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ fetchImpl });
  const result = await deleteOddsSnapshotsChunked(config);
  expect(result.totalDeleted).toBe(0);
});

it("treats null resume value as zero", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: null }))
    .mockResolvedValueOnce(buildResponse({ deleted: 0, done: true, next_since_id: 0 }))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ fetchImpl });
  const result = await deleteOddsSnapshotsChunked(config);
  expect(result.totalDeleted).toBe(0);
});

it("retries on saturation and surfaces error after retry limit", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValue(buildResponse({ error: "saturated" }, { status: 429 }));
  const config = buildConfig({ fetchImpl, retryLimit: 3 });
  await expect(deleteOddsSnapshotsChunked(config)).rejects.toThrow();
});

it("throws when delete-odds-chunk returns non-429 non-ok status", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValue(buildResponse({ error: "bad" }, { status: 500 }));
  const config = buildConfig({ fetchImpl, retryLimit: 1 });
  await expect(deleteOddsSnapshotsChunked(config)).rejects.toThrow();
});

it("throws when retryLimit is zero", async () => {
  const fetchImpl = vi.fn<typeof fetch>().mockResolvedValueOnce(buildResponse({ value: "0" }));
  const config = buildConfig({ fetchImpl, retryLimit: 0 });
  await expect(deleteOddsSnapshotsChunked(config)).rejects.toThrow();
});

it("assertDeleteConfirmed throws when CONFIRM_DELETE is unset", () => {
  delete process.env.CONFIRM_DELETE;
  expect(() => assertDeleteConfirmed()).toThrow();
});

it("assertDeleteConfirmed throws when CONFIRM_DELETE is not 1", () => {
  process.env.CONFIRM_DELETE = "yes";
  expect(() => assertDeleteConfirmed()).toThrow();
});

it("assertDeleteConfirmed does not throw when CONFIRM_DELETE is 1", () => {
  process.env.CONFIRM_DELETE = "1";
  expect(() => assertDeleteConfirmed()).not.toThrow();
});

it("buildDefaultConfig throws when CONFIRM_DELETE is not 1", async () => {
  delete process.env.CONFIRM_DELETE;
  const fetchImpl = vi.fn(async () => buildResponse({ value: "1500" }));
  await expect(buildDefaultConfig(new Date("2026-05-28T15:00:00Z"), fetchImpl)).rejects.toThrow();
});

it("buildDefaultConfig reads env vars and fetches upper bound", async () => {
  process.env.CONFIRM_DELETE = "1";
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  const fetchImpl = vi.fn(async () => buildResponse({ value: "1500" }));
  const config = await buildDefaultConfig(new Date("2026-05-28T15:00:00Z"), fetchImpl);
  expect(config.upperBoundId).toBe(1500);
});

it("buildDefaultConfig throws when upper bound endpoint returns non-ok", async () => {
  process.env.CONFIRM_DELETE = "1";
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  const fetchImpl = vi.fn(async () => buildResponse({ error: "x" }, { status: 401 }));
  await expect(buildDefaultConfig(new Date(), fetchImpl)).rejects.toThrow();
});

it("buildDefaultConfig throws when upper bound value is null", async () => {
  process.env.CONFIRM_DELETE = "1";
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  const fetchImpl = vi.fn(async () => buildResponse({ value: null }));
  await expect(buildDefaultConfig(new Date(), fetchImpl)).rejects.toThrow();
});

it("buildDefaultConfig throws when upper bound value is invalid", async () => {
  process.env.CONFIRM_DELETE = "1";
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  const fetchImpl = vi.fn(async () => buildResponse({ value: "not-a-number" }));
  await expect(buildDefaultConfig(new Date(), fetchImpl)).rejects.toThrow();
});

it("buildDefaultConfig throws when env var missing", async () => {
  process.env.CONFIRM_DELETE = "1";
  delete process.env.REALTIME_ADMIN_TOKEN;
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  const fetchImpl = vi.fn(async () => buildResponse({ value: "1500" }));
  await expect(buildDefaultConfig(new Date(), fetchImpl)).rejects.toThrow();
});
