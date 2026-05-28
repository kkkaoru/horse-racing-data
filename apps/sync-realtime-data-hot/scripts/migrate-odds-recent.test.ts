// Run with bun.
import { expect, it, vi } from "vitest";

import {
  buildDefaultConfig,
  migrateOddsRecent,
  type ExportChunkResponse,
  type MigrateOddsRecentConfig,
} from "./migrate-odds-recent";

const buildConfig = (
  overrides: Partial<MigrateOddsRecentConfig> = {},
): MigrateOddsRecentConfig => ({
  adminToken: "admin",
  afterFetchedAt: "2026-05-28T00:00:00.000Z",
  batchSize: 200,
  circuitPauseMs: 1,
  fetchImpl: vi.fn(),
  internalToken: "internal",
  newWorkerUrl: "https://new.example.com",
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

it("returns zero counts when the first chunk has zero rows", async () => {
  const fetchImpl = vi.fn(async () => buildResponse({ done: true, next_since_id: 0, rows: [] }));
  const config = buildConfig({ fetchImpl });
  const result = await migrateOddsRecent(config);
  expect(result).toStrictEqual({ maxId: 0, totalInserted: 0 });
  expect(fetchImpl).toHaveBeenCalledTimes(2);
});

it("forwards chunks and persists the final max id", async () => {
  const chunkOne: ExportChunkResponse = {
    done: false,
    next_since_id: 5,
    rows: [
      {
        average_odds: null,
        combination: "01",
        fetched_at: "2026-05-28T10:00:00+09:00",
        id: 5,
        max_odds: null,
        min_odds: null,
        odds: 2.5,
        odds_type: "tansho",
        race_key: "nar:20260528:42:01",
        rank: 1,
      },
    ],
  };
  const chunkTwo: ExportChunkResponse = { done: true, next_since_id: 5, rows: [] };
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse(chunkOne))
    .mockResolvedValueOnce(buildResponse({ inserted: 1 }))
    .mockResolvedValueOnce(buildResponse(chunkTwo))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ fetchImpl });
  const result = await migrateOddsRecent(config);
  expect(result.totalInserted).toBe(1);
  expect(result.maxId).toBe(5);
});

it("stops after a done chunk even when rows remain", async () => {
  const chunk: ExportChunkResponse = {
    done: true,
    next_since_id: 10,
    rows: [
      {
        average_odds: null,
        combination: "01",
        fetched_at: "2026-05-28T10:00:00+09:00",
        id: 10,
        max_odds: null,
        min_odds: null,
        odds: 2.5,
        odds_type: "tansho",
        race_key: "nar:20260528:42:01",
        rank: 1,
      },
    ],
  };
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse(chunk))
    .mockResolvedValueOnce(buildResponse({ inserted: 1 }))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ fetchImpl });
  const result = await migrateOddsRecent(config);
  expect(result.maxId).toBe(10);
});

it("retries on 429 with backoff and surfaces error after the limit", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ error: "saturated" }, { status: 429 }))
    .mockResolvedValueOnce(buildResponse({ error: "saturated" }, { status: 429 }))
    .mockResolvedValue(buildResponse({ error: "saturated" }, { status: 429 }));
  const sleepImpl = vi.fn(async () => undefined);
  const config = buildConfig({ fetchImpl, retryLimit: 3, sleepImpl });
  await expect(migrateOddsRecent(config)).rejects.toThrow();
  expect(sleepImpl).toHaveBeenCalled();
});

it("throws when import-odds-chunk returns non-ok", async () => {
  const chunk: ExportChunkResponse = {
    done: false,
    next_since_id: 5,
    rows: [
      {
        average_odds: null,
        combination: "01",
        fetched_at: "2026-05-28T10:00:00+09:00",
        id: 5,
        max_odds: null,
        min_odds: null,
        odds: 2.5,
        odds_type: "tansho",
        race_key: "nar:20260528:42:01",
        rank: 1,
      },
    ],
  };
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse(chunk))
    .mockResolvedValueOnce(buildResponse({ error: "bad" }, { status: 500 }));
  const config = buildConfig({ fetchImpl });
  await expect(migrateOddsRecent(config)).rejects.toThrow();
});

it("throws when export-odds-chunk returns non-ok status that is not 429", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValue(buildResponse({ error: "forbidden" }, { status: 403 }));
  const config = buildConfig({ fetchImpl, retryLimit: 1 });
  await expect(migrateOddsRecent(config)).rejects.toThrow();
});

it("buildDefaultConfig reads env vars and computes after_fetched_at from lookback", () => {
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  process.env.MIGRATION_LOOKBACK_HOURS = "12";
  const config = buildDefaultConfig(new Date("2026-05-28T12:00:00Z"));
  expect(config.afterFetchedAt).toBe("2026-05-28T00:00:00.000Z");
});

it("buildDefaultConfig falls back to default lookback hours when env value is invalid", () => {
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  process.env.MIGRATION_LOOKBACK_HOURS = "bad";
  const config = buildDefaultConfig(new Date("2026-05-28T12:00:00Z"));
  expect(config.afterFetchedAt).toBe("2026-05-27T12:00:00.000Z");
});

it("buildDefaultConfig uses default lookback when env var unset", () => {
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  delete process.env.MIGRATION_LOOKBACK_HOURS;
  const config = buildDefaultConfig(new Date("2026-05-28T12:00:00Z"));
  expect(config.afterFetchedAt).toBe("2026-05-27T12:00:00.000Z");
});

it("throws when retryLimit is zero so the loop never executes", async () => {
  const fetchImpl = vi.fn();
  const config = buildConfig({ fetchImpl, retryLimit: 0 });
  await expect(migrateOddsRecent(config)).rejects.toThrow();
});

it("buildDefaultConfig throws when required env var missing", () => {
  delete process.env.REALTIME_ADMIN_TOKEN;
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  expect(() => buildDefaultConfig(new Date())).toThrow();
});
