// Run with bun.
import { expect, it, vi } from "vitest";

import {
  buildDefaultConfig,
  seedOddsFetchState,
  type ExportRaceSourcesChunkResponse,
  type SeedOddsFetchStateConfig,
} from "./seed-odds-fetch-state";

const buildConfig = (
  overrides: Partial<SeedOddsFetchStateConfig> = {},
): SeedOddsFetchStateConfig => ({
  adminToken: "admin",
  batchSize: 50,
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

const buildRow = (overrides: Record<string, unknown> = {}) => ({
  deba_url: "https://x.test/race",
  kaisai_nen: "2026",
  kaisai_tsukihi: "0529",
  keibajo_code: "08",
  odds_links_json: "{}",
  race_bango: "01",
  race_key: "jra:2026:0529:08:01",
  race_start_at_jst: "2026-05-29T13:00:00+09:00",
  rowid: 1,
  source: "jra" as const,
  ...overrides,
});

it("returns zero counts when the first chunk has zero rows", async () => {
  const fetchImpl = vi.fn(async () => buildResponse({ done: true, next_since_id: 0, rows: [] }));
  const config = buildConfig({ fetchImpl });
  const result = await seedOddsFetchState(config);
  expect(result).toStrictEqual({ maxRowid: 0, totalSeeded: 0 });
  expect(fetchImpl).toHaveBeenCalledTimes(2);
});

it("forwards a single batch and stops when done=true", async () => {
  const chunk: ExportRaceSourcesChunkResponse = {
    done: true,
    next_since_id: 7,
    rows: [buildRow({ rowid: 7 })],
  };
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse(chunk))
    .mockResolvedValueOnce(buildResponse({ ok: true }))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ fetchImpl });
  const result = await seedOddsFetchState(config);
  expect(result).toStrictEqual({ maxRowid: 7, totalSeeded: 1 });
});

it("forwards multiple batches and persists the final max rowid", async () => {
  const chunkOne: ExportRaceSourcesChunkResponse = {
    done: false,
    next_since_id: 5,
    rows: [buildRow({ rowid: 5 })],
  };
  const chunkTwo: ExportRaceSourcesChunkResponse = {
    done: false,
    next_since_id: 9,
    rows: [buildRow({ race_key: "jra:2026:0529:08:02", race_bango: "02", rowid: 9 })],
  };
  const chunkThree: ExportRaceSourcesChunkResponse = {
    done: true,
    next_since_id: 9,
    rows: [],
  };
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse(chunkOne))
    .mockResolvedValueOnce(buildResponse({ ok: true }))
    .mockResolvedValueOnce(buildResponse(chunkTwo))
    .mockResolvedValueOnce(buildResponse({ ok: true }))
    .mockResolvedValueOnce(buildResponse(chunkThree))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ fetchImpl });
  const result = await seedOddsFetchState(config);
  expect(result.totalSeeded).toBe(2);
  expect(result.maxRowid).toBe(9);
});

it("stops after a done chunk even when rows remain in that response", async () => {
  const chunk: ExportRaceSourcesChunkResponse = {
    done: true,
    next_since_id: 12,
    rows: [buildRow({ rowid: 12 })],
  };
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse(chunk))
    .mockResolvedValueOnce(buildResponse({ ok: true }))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ fetchImpl });
  const result = await seedOddsFetchState(config);
  expect(result.maxRowid).toBe(12);
});

it("retries on 429 with backoff and surfaces error after the limit", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValue(buildResponse({ error: "saturated" }, { status: 429 }));
  const sleepImpl = vi.fn(async () => undefined);
  const config = buildConfig({ fetchImpl, retryLimit: 3, sleepImpl });
  await expect(seedOddsFetchState(config)).rejects.toThrow();
  expect(sleepImpl).toHaveBeenCalled();
});

it("throws when forwardRow receives non-ok from new worker", async () => {
  const chunk: ExportRaceSourcesChunkResponse = {
    done: false,
    next_since_id: 5,
    rows: [buildRow({ rowid: 5 })],
  };
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse(chunk))
    .mockResolvedValueOnce(buildResponse({ error: "bad" }, { status: 500 }));
  const config = buildConfig({ fetchImpl });
  await expect(seedOddsFetchState(config)).rejects.toThrow();
});

it("throws when export-race-sources-chunk returns non-ok status that is not 429", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValue(buildResponse({ error: "forbidden" }, { status: 403 }));
  const config = buildConfig({ fetchImpl, retryLimit: 1 });
  await expect(seedOddsFetchState(config)).rejects.toThrow();
});

it("throws when retryLimit is zero so the loop never executes", async () => {
  const fetchImpl = vi.fn();
  const config = buildConfig({ fetchImpl, retryLimit: 0 });
  await expect(seedOddsFetchState(config)).rejects.toThrow();
});

it("buildDefaultConfig reads env vars and returns expected URL fields", () => {
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  const config = buildDefaultConfig(new Date("2026-05-29T12:00:00Z"));
  expect(config.oldWorkerUrl).toBe("https://old.example.com");
  expect(config.newWorkerUrl).toBe("https://new.example.com");
  expect(config.adminToken).toBe("admin");
  expect(config.internalToken).toBe("internal");
});

it("buildDefaultConfig throws when REALTIME_ADMIN_TOKEN is missing", () => {
  delete process.env.REALTIME_ADMIN_TOKEN;
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  expect(() => buildDefaultConfig(new Date())).toThrow();
});

it("buildDefaultConfig throws when PC_KEIBA_VIEWER_INTERNAL_TOKEN is missing", () => {
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  delete process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN;
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  expect(() => buildDefaultConfig(new Date())).toThrow();
});

it("buildDefaultConfig throws when NEW_WORKER_URL is missing", () => {
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  delete process.env.NEW_WORKER_URL;
  process.env.OLD_WORKER_URL = "https://old.example.com";
  expect(() => buildDefaultConfig(new Date())).toThrow();
});

it("buildDefaultConfig throws when OLD_WORKER_URL is missing", () => {
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  delete process.env.OLD_WORKER_URL;
  expect(() => buildDefaultConfig(new Date())).toThrow();
});
