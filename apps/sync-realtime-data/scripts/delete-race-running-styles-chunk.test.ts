// Run with bun.
import { expect, it, vi } from "vitest";

import {
  assertDeleteConfirmed,
  buildDefaultConfig,
  deleteRaceRunningStylesChunked,
  isWithinNightWindow,
  readPositiveIntEnv,
  type DeleteRaceRunningStylesChunkConfig,
  type DeleteRaceRunningStylesChunkResponse,
} from "./delete-race-running-styles-chunk";

const KNOB_NAMES = [
  "CHUNK_SIZE",
  "CHUNK_DELAY_MS",
  "RETRY_LIMIT",
  "RETRY_BACKOFF_MS",
  "CIRCUIT_PAUSE_MS",
];

const clearKnobs = (): void => {
  delete process.env.CHUNK_SIZE;
  delete process.env.CHUNK_DELAY_MS;
  delete process.env.RETRY_LIMIT;
  delete process.env.RETRY_BACKOFF_MS;
  delete process.env.CIRCUIT_PAUSE_MS;
};

const setRequiredEnv = (): void => {
  process.env.CONFIRM_DELETE = "1";
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.FEATURES_WORKER_URL = "https://features.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
};

const buildConfig = (
  overrides: Partial<DeleteRaceRunningStylesChunkConfig> = {},
): DeleteRaceRunningStylesChunkConfig => ({
  adminToken: "admin",
  batchSize: 500,
  circuitPauseMs: 1,
  featuresWorkerUrl: "https://features.example.com",
  fetchImpl: vi.fn(),
  internalToken: "internal",
  nowImpl: () => new Date("2026-05-28T15:00:00Z"),
  oldWorkerUrl: "https://old.example.com",
  retryBackoffMs: 1,
  retryLimit: 3,
  sleepImpl: vi.fn(async () => undefined),
  sleepMs: 1,
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

it("isWithinNightWindow returns true at JST 04:59", () => {
  expect(isWithinNightWindow(new Date("2026-05-28T19:59:00Z"))).toBe(true);
});

it("isWithinNightWindow returns false at JST 05:00", () => {
  expect(isWithinNightWindow(new Date("2026-05-28T20:00:00Z"))).toBe(false);
});

it("isWithinNightWindow returns false at JST 12:00", () => {
  expect(isWithinNightWindow(new Date("2026-05-28T03:00:00Z"))).toBe(false);
});

it("isWithinNightWindow returns false at JST 22:00", () => {
  expect(isWithinNightWindow(new Date("2026-05-28T13:00:00Z"))).toBe(false);
});

it("deleteRaceRunningStylesChunked stops immediately when outside night window", async () => {
  const fetchImpl = vi.fn(async () => buildResponse({ value: "0" }));
  const config = buildConfig({
    fetchImpl,
    nowImpl: () => new Date("2026-05-28T03:00:00Z"),
  });
  const result = await deleteRaceRunningStylesChunked(config);
  expect(result.stoppedReason).toBe("outside-night-window");
  expect(result.totalDeleted).toBe(0);
});

it("deleteRaceRunningStylesChunked deletes a chunk and saves cursor after each chunk", async () => {
  const chunkOne: DeleteRaceRunningStylesChunkResponse = {
    deletedRowCount: 500,
    nextSinceRowid: 500,
  };
  const chunkTwo: DeleteRaceRunningStylesChunkResponse = {
    deletedRowCount: 100,
    nextSinceRowid: 600,
  };
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValueOnce(buildResponse(chunkOne))
    .mockResolvedValueOnce(buildResponse({ ok: true }))
    .mockResolvedValueOnce(buildResponse(chunkTwo))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ fetchImpl });
  const result = await deleteRaceRunningStylesChunked(config);
  expect(result.stoppedReason).toBe("completed");
  expect(result.totalDeleted).toBe(600);
  expect(result.finalSinceRowid).toBe(600);
});

it("deleteRaceRunningStylesChunked resumes from saved cursor", async () => {
  const chunk: DeleteRaceRunningStylesChunkResponse = {
    deletedRowCount: 0,
    nextSinceRowid: 12345,
  };
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "12345" }))
    .mockResolvedValueOnce(buildResponse(chunk))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ fetchImpl });
  const result = await deleteRaceRunningStylesChunked(config);
  expect(result.totalDeleted).toBe(0);
  expect(result.finalSinceRowid).toBe(12345);
});

it("deleteRaceRunningStylesChunked treats non-ok cursor response as zero", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ error: "x" }, { status: 404 }))
    .mockResolvedValueOnce(buildResponse({ deletedRowCount: 0, nextSinceRowid: 0 }))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ fetchImpl });
  const result = await deleteRaceRunningStylesChunked(config);
  expect(result.totalDeleted).toBe(0);
  expect(result.finalSinceRowid).toBe(0);
});

it("deleteRaceRunningStylesChunked treats null cursor value as zero", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: null }))
    .mockResolvedValueOnce(buildResponse({ deletedRowCount: 0, nextSinceRowid: 0 }))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ fetchImpl });
  const result = await deleteRaceRunningStylesChunked(config);
  expect(result.totalDeleted).toBe(0);
});

it("deleteRaceRunningStylesChunked treats non-numeric cursor value as zero", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "garbage" }))
    .mockResolvedValueOnce(buildResponse({ deletedRowCount: 0, nextSinceRowid: 0 }))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ fetchImpl });
  const result = await deleteRaceRunningStylesChunked(config);
  expect(result.totalDeleted).toBe(0);
});

it("deleteRaceRunningStylesChunked treats negative cursor value as zero", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "-5" }))
    .mockResolvedValueOnce(buildResponse({ deletedRowCount: 0, nextSinceRowid: 0 }))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ fetchImpl });
  const result = await deleteRaceRunningStylesChunked(config);
  expect(result.totalDeleted).toBe(0);
});

it("deleteRaceRunningStylesChunked retries on 7429 backoff and surfaces error after circuit breaker", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValue(buildResponse({ error: "saturated" }, { status: 429 }));
  const config = buildConfig({ fetchImpl, retryLimit: 3 });
  await expect(deleteRaceRunningStylesChunked(config)).rejects.toThrow();
});

it("deleteRaceRunningStylesChunked circuit breaker pauses for circuitPauseMs", async () => {
  const sleepImpl = vi.fn(async () => undefined);
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValue(buildResponse({ error: "down" }, { status: 500 }));
  const config = buildConfig({ circuitPauseMs: 12345, fetchImpl, retryLimit: 1, sleepImpl });
  await expect(deleteRaceRunningStylesChunked(config)).rejects.toThrow();
  expect(sleepImpl).toHaveBeenCalledWith(12345);
});

it("deleteRaceRunningStylesChunked accumulates totalDeleted across many chunks", async () => {
  const chunkA: DeleteRaceRunningStylesChunkResponse = {
    deletedRowCount: 500,
    nextSinceRowid: 500,
  };
  const chunkB: DeleteRaceRunningStylesChunkResponse = {
    deletedRowCount: 500,
    nextSinceRowid: 1000,
  };
  const chunkC: DeleteRaceRunningStylesChunkResponse = {
    deletedRowCount: 42,
    nextSinceRowid: 1042,
  };
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValueOnce(buildResponse(chunkA))
    .mockResolvedValueOnce(buildResponse({ ok: true }))
    .mockResolvedValueOnce(buildResponse(chunkB))
    .mockResolvedValueOnce(buildResponse({ ok: true }))
    .mockResolvedValueOnce(buildResponse(chunkC))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ fetchImpl });
  const result = await deleteRaceRunningStylesChunked(config);
  expect(result.stoppedReason).toBe("completed");
  expect(result.totalDeleted).toBe(1042);
});

it("deleteRaceRunningStylesChunked throws when retryLimit is zero", async () => {
  const fetchImpl = vi.fn<typeof fetch>().mockResolvedValueOnce(buildResponse({ value: "0" }));
  const config = buildConfig({ fetchImpl, retryLimit: 0 });
  await expect(deleteRaceRunningStylesChunked(config)).rejects.toThrow();
});

it("deleteRaceRunningStylesChunked throws on non-429 non-ok status", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValue(buildResponse({ error: "bad" }, { status: 500 }));
  const config = buildConfig({ fetchImpl, retryLimit: 1 });
  await expect(deleteRaceRunningStylesChunked(config)).rejects.toThrow();
});

const readBodyString = (init: RequestInit | undefined): string =>
  typeof init?.body === "string" ? init.body : "";

it("deleteRaceRunningStylesChunked posts since_rowid and chunk_size to the delete endpoint", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "100" }))
    .mockResolvedValueOnce(buildResponse({ deletedRowCount: 0, nextSinceRowid: 100 }))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ batchSize: 250, fetchImpl });
  await deleteRaceRunningStylesChunked(config);
  const deleteCall = fetchImpl.mock.calls[1];
  expect(deleteCall?.[0]).toBe(
    "https://old.example.com/api/internal/delete-race-running-styles-chunk",
  );
  expect(JSON.parse(readBodyString(deleteCall?.[1]))).toStrictEqual({
    chunk_size: 250,
    since_rowid: 100,
  });
});

it("deleteRaceRunningStylesChunked sends features:cleanup cursor key to migration-state", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValueOnce(buildResponse({ deletedRowCount: 0, nextSinceRowid: 0 }))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ fetchImpl });
  await deleteRaceRunningStylesChunked(config);
  const cursorCall = fetchImpl.mock.calls[2];
  expect(cursorCall?.[0]).toBe("https://features.example.com/api/internal/migration-state");
  expect(JSON.parse(readBodyString(cursorCall?.[1]))).toStrictEqual({
    key: "features:cleanup:race-running-styles-cursor",
    value: "0",
  });
});

it("assertDeleteConfirmed throws when CONFIRM_DELETE is unset", () => {
  delete process.env.CONFIRM_DELETE;
  expect(() => assertDeleteConfirmed()).toThrow(
    "Refusing to delete: set CONFIRM_DELETE=1 to acknowledge irreversibility",
  );
});

it("assertDeleteConfirmed throws when CONFIRM_DELETE is not 1", () => {
  process.env.CONFIRM_DELETE = "yes";
  expect(() => assertDeleteConfirmed()).toThrow();
});

it("assertDeleteConfirmed does not throw when CONFIRM_DELETE is 1", () => {
  process.env.CONFIRM_DELETE = "1";
  expect(() => assertDeleteConfirmed()).not.toThrow();
});

it("buildDefaultConfig throws when CONFIRM_DELETE is not 1", () => {
  delete process.env.CONFIRM_DELETE;
  expect(() => buildDefaultConfig(new Date("2026-05-28T15:00:00Z"), globalThis.fetch)).toThrow();
});

it("buildDefaultConfig reads env vars when CONFIRM_DELETE=1", () => {
  clearKnobs();
  setRequiredEnv();
  const config = buildDefaultConfig(new Date("2026-05-28T15:00:00Z"), globalThis.fetch);
  expect(config.oldWorkerUrl).toBe("https://old.example.com");
  expect(config.batchSize).toBe(500);
  expect(config.sleepMs).toBe(3000);
});

it("buildDefaultConfig throws when REALTIME_ADMIN_TOKEN missing", () => {
  process.env.CONFIRM_DELETE = "1";
  delete process.env.REALTIME_ADMIN_TOKEN;
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.FEATURES_WORKER_URL = "https://features.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  expect(() => buildDefaultConfig(new Date(), globalThis.fetch)).toThrow();
});

it("buildDefaultConfig nowImpl returns the now passed in", () => {
  process.env.CONFIRM_DELETE = "1";
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.FEATURES_WORKER_URL = "https://features.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  const now = new Date("2026-05-28T15:00:00Z");
  const config = buildDefaultConfig(now, globalThis.fetch);
  expect(config.nowImpl().toISOString()).toBe("2026-05-28T15:00:00.000Z");
});

it("buildDefaultConfig sleepImpl is the real setTimeout-based sleep", async () => {
  clearKnobs();
  setRequiredEnv();
  const config = buildDefaultConfig(new Date(), globalThis.fetch);
  await config.sleepImpl(0);
});

it("buildDefaultConfig uses default knobs when env vars unset", () => {
  clearKnobs();
  setRequiredEnv();
  const config = buildDefaultConfig(new Date("2026-05-28T15:00:00Z"), globalThis.fetch);
  expect(config.batchSize).toBe(500);
  expect(config.sleepMs).toBe(3000);
  expect(config.retryLimit).toBe(3);
  expect(config.retryBackoffMs).toBe(60_000);
  expect(config.circuitPauseMs).toBe(1_800_000);
});

it("buildDefaultConfig overrides CHUNK_SIZE when env var is set", () => {
  clearKnobs();
  setRequiredEnv();
  process.env.CHUNK_SIZE = "100";
  const config = buildDefaultConfig(new Date("2026-05-28T15:00:00Z"), globalThis.fetch);
  expect(config.batchSize).toBe(100);
});

it("buildDefaultConfig overrides CHUNK_DELAY_MS when env var is set", () => {
  clearKnobs();
  setRequiredEnv();
  process.env.CHUNK_DELAY_MS = "10000";
  const config = buildDefaultConfig(new Date("2026-05-28T15:00:00Z"), globalThis.fetch);
  expect(config.sleepMs).toBe(10_000);
});

it("buildDefaultConfig overrides RETRY_LIMIT when env var is set", () => {
  clearKnobs();
  setRequiredEnv();
  process.env.RETRY_LIMIT = "5";
  const config = buildDefaultConfig(new Date("2026-05-28T15:00:00Z"), globalThis.fetch);
  expect(config.retryLimit).toBe(5);
});

it("buildDefaultConfig overrides RETRY_BACKOFF_MS when env var is set", () => {
  clearKnobs();
  setRequiredEnv();
  process.env.RETRY_BACKOFF_MS = "0";
  const config = buildDefaultConfig(new Date("2026-05-28T15:00:00Z"), globalThis.fetch);
  expect(config.retryBackoffMs).toBe(0);
});

it("buildDefaultConfig overrides CIRCUIT_PAUSE_MS when env var is set", () => {
  clearKnobs();
  setRequiredEnv();
  process.env.CIRCUIT_PAUSE_MS = "0";
  const config = buildDefaultConfig(new Date("2026-05-28T15:00:00Z"), globalThis.fetch);
  expect(config.circuitPauseMs).toBe(0);
});

it("buildDefaultConfig throws when CHUNK_SIZE is non-numeric", () => {
  clearKnobs();
  setRequiredEnv();
  process.env.CHUNK_SIZE = "abc";
  expect(() => buildDefaultConfig(new Date("2026-05-28T15:00:00Z"), globalThis.fetch)).toThrow(
    "CHUNK_SIZE must be a positive integer (>= 1)",
  );
});

it("buildDefaultConfig throws when CHUNK_SIZE is zero", () => {
  clearKnobs();
  setRequiredEnv();
  process.env.CHUNK_SIZE = "0";
  expect(() => buildDefaultConfig(new Date("2026-05-28T15:00:00Z"), globalThis.fetch)).toThrow(
    "CHUNK_SIZE must be a positive integer (>= 1)",
  );
});

it("buildDefaultConfig throws when CHUNK_SIZE is negative", () => {
  clearKnobs();
  setRequiredEnv();
  process.env.CHUNK_SIZE = "-5";
  expect(() => buildDefaultConfig(new Date("2026-05-28T15:00:00Z"), globalThis.fetch)).toThrow(
    "CHUNK_SIZE must be a positive integer (>= 1)",
  );
});

it("buildDefaultConfig throws when CHUNK_DELAY_MS is negative", () => {
  clearKnobs();
  setRequiredEnv();
  process.env.CHUNK_DELAY_MS = "-1";
  expect(() => buildDefaultConfig(new Date("2026-05-28T15:00:00Z"), globalThis.fetch)).toThrow(
    "CHUNK_DELAY_MS must be a positive integer (>= 0)",
  );
});

it("buildDefaultConfig throws when CHUNK_DELAY_MS is non-numeric", () => {
  clearKnobs();
  setRequiredEnv();
  process.env.CHUNK_DELAY_MS = "abc";
  expect(() => buildDefaultConfig(new Date("2026-05-28T15:00:00Z"), globalThis.fetch)).toThrow(
    "CHUNK_DELAY_MS must be a positive integer (>= 0)",
  );
});

it("buildDefaultConfig throws when RETRY_LIMIT is zero", () => {
  clearKnobs();
  setRequiredEnv();
  process.env.RETRY_LIMIT = "0";
  expect(() => buildDefaultConfig(new Date("2026-05-28T15:00:00Z"), globalThis.fetch)).toThrow(
    "RETRY_LIMIT must be a positive integer (>= 1)",
  );
});

it("readPositiveIntEnv falls back when env var is empty string", () => {
  clearKnobs();
  process.env.CHUNK_SIZE = "";
  expect(readPositiveIntEnv("CHUNK_SIZE", 42, 1)).toBe(42);
});

it("readPositiveIntEnv throws when value is a float", () => {
  clearKnobs();
  process.env.RETRY_BACKOFF_MS = "1.5";
  expect(() => readPositiveIntEnv("RETRY_BACKOFF_MS", 1, 0)).toThrow(
    "RETRY_BACKOFF_MS must be a positive integer (>= 0)",
  );
});

it("KNOB_NAMES enumerates the 5 documented env knobs", () => {
  expect(KNOB_NAMES).toStrictEqual([
    "CHUNK_SIZE",
    "CHUNK_DELAY_MS",
    "RETRY_LIMIT",
    "RETRY_BACKOFF_MS",
    "CIRCUIT_PAUSE_MS",
  ]);
});
