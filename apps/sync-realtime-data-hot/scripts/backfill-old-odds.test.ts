// Run with bun.
import { expect, it, vi } from "vitest";

import {
  backfillOldOdds,
  buildDefaultConfig,
  isWithinNightWindow,
  type BackfillChunkResponse,
  type BackfillOldOddsConfig,
} from "./backfill-old-odds";

const buildConfig = (overrides: Partial<BackfillOldOddsConfig> = {}): BackfillOldOddsConfig => ({
  adminToken: "admin",
  batchSize: 200,
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
  upperBoundId: 100,
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
  const result = await backfillOldOdds(config);
  expect(result.stoppedReason).toBe("outside-night-window");
  expect(result.totalInserted).toBe(0);
});

it("stops when no rows match the upper bound", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValueOnce(
      buildResponse({
        done: false,
        next_since_id: 150,
        rows: [
          {
            average_odds: null,
            combination: "01",
            fetched_at: "2026-05-28T10:00:00+09:00",
            id: 150,
            max_odds: null,
            min_odds: null,
            odds: 2.5,
            odds_type: "tansho",
            race_key: "nar:20260528:42:01",
            rank: 1,
          },
        ],
      }),
    );
  const config = buildConfig({ fetchImpl, upperBoundId: 100 });
  const result = await backfillOldOdds(config);
  expect(result.stoppedReason).toBe("upper-bound-reached");
});

it("stops when chunk has empty rows", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValueOnce(buildResponse({ done: true, next_since_id: 0, rows: [] }));
  const config = buildConfig({ fetchImpl });
  const result = await backfillOldOdds(config);
  expect(result.stoppedReason).toBe("completed");
});

it("imports a chunk and persists progress when within bounds", async () => {
  const chunk: BackfillChunkResponse = {
    done: false,
    next_since_id: 50,
    rows: [
      {
        average_odds: null,
        combination: "01",
        fetched_at: "2026-05-28T10:00:00+09:00",
        id: 50,
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
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValueOnce(buildResponse(chunk))
    .mockResolvedValueOnce(buildResponse({ inserted: 1 }))
    .mockResolvedValueOnce(buildResponse({ ok: true }))
    .mockResolvedValueOnce(buildResponse({ done: true, next_since_id: 50, rows: [] }));
  const config = buildConfig({ fetchImpl });
  const result = await backfillOldOdds(config);
  expect(result.totalInserted).toBe(1);
  expect(result.finalSinceId).toBe(50);
});

it("treats done chunk as completion after import", async () => {
  const chunk: BackfillChunkResponse = {
    done: true,
    next_since_id: 60,
    rows: [
      {
        average_odds: null,
        combination: "01",
        fetched_at: "2026-05-28T10:00:00+09:00",
        id: 60,
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
    .mockResolvedValueOnce(buildResponse({ value: null }))
    .mockResolvedValueOnce(buildResponse(chunk))
    .mockResolvedValueOnce(buildResponse({ inserted: 1 }))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ fetchImpl });
  const result = await backfillOldOdds(config);
  expect(result.stoppedReason).toBe("completed");
});

it("treats non-ok resume read as starting from zero", async () => {
  const chunk: BackfillChunkResponse = { done: true, next_since_id: 0, rows: [] };
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ error: "missing" }, { status: 404 }))
    .mockResolvedValueOnce(buildResponse(chunk));
  const config = buildConfig({ fetchImpl });
  const result = await backfillOldOdds(config);
  expect(result.totalInserted).toBe(0);
});

it("falls back to zero when resume value is non-numeric", async () => {
  const chunk: BackfillChunkResponse = { done: true, next_since_id: 0, rows: [] };
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "junk" }))
    .mockResolvedValueOnce(buildResponse(chunk));
  const config = buildConfig({ fetchImpl });
  const result = await backfillOldOdds(config);
  expect(result.totalInserted).toBe(0);
});

it("filters rows above the upper bound and stops", async () => {
  const chunk: BackfillChunkResponse = {
    done: false,
    next_since_id: 120,
    rows: [
      {
        average_odds: null,
        combination: "01",
        fetched_at: "2026-05-28T10:00:00+09:00",
        id: 50,
        max_odds: null,
        min_odds: null,
        odds: 2.5,
        odds_type: "tansho",
        race_key: "nar:20260528:42:01",
        rank: 1,
      },
      {
        average_odds: null,
        combination: "02",
        fetched_at: "2026-05-28T10:00:00+09:00",
        id: 150,
        max_odds: null,
        min_odds: null,
        odds: 5.0,
        odds_type: "tansho",
        race_key: "nar:20260528:42:01",
        rank: 2,
      },
    ],
  };
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValueOnce(buildResponse(chunk))
    .mockResolvedValueOnce(buildResponse({ inserted: 1 }))
    .mockResolvedValueOnce(buildResponse({ ok: true }))
    .mockResolvedValueOnce(buildResponse({ done: false, next_since_id: 50, rows: [] }));
  const config = buildConfig({ fetchImpl, upperBoundId: 100 });
  const result = await backfillOldOdds(config);
  expect(result.totalInserted).toBe(1);
});

it("retries on saturation and surfaces error after retry limit", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValueOnce(buildResponse({ error: "saturated" }, { status: 429 }))
    .mockResolvedValueOnce(buildResponse({ error: "saturated" }, { status: 429 }))
    .mockResolvedValue(buildResponse({ error: "saturated" }, { status: 429 }));
  const config = buildConfig({ fetchImpl, retryLimit: 3 });
  await expect(backfillOldOdds(config)).rejects.toThrow();
});

it("throws when import-odds-chunk returns non-ok", async () => {
  const chunk: BackfillChunkResponse = {
    done: false,
    next_since_id: 50,
    rows: [
      {
        average_odds: null,
        combination: "01",
        fetched_at: "2026-05-28T10:00:00+09:00",
        id: 50,
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
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValueOnce(buildResponse(chunk))
    .mockResolvedValueOnce(buildResponse({ error: "bad" }, { status: 500 }));
  const config = buildConfig({ fetchImpl });
  await expect(backfillOldOdds(config)).rejects.toThrow();
});

it("throws when retryLimit is zero", async () => {
  const fetchImpl = vi.fn<typeof fetch>().mockResolvedValueOnce(buildResponse({ value: "0" }));
  const config = buildConfig({ fetchImpl, retryLimit: 0 });
  await expect(backfillOldOdds(config)).rejects.toThrow();
});

it("throws when export-odds-chunk returns non-429 non-ok", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValue(buildResponse({ error: "forbidden" }, { status: 403 }));
  const config = buildConfig({ fetchImpl, retryLimit: 1 });
  await expect(backfillOldOdds(config)).rejects.toThrow();
});

it("stops immediately when resume id already meets upper bound", async () => {
  const fetchImpl = vi.fn(async () => buildResponse({ value: "200" }));
  const config = buildConfig({ fetchImpl, upperBoundId: 100 });
  const result = await backfillOldOdds(config);
  expect(result.stoppedReason).toBe("upper-bound-reached");
  expect(result.totalInserted).toBe(0);
});

it("buildDefaultConfig fetches b1-max-id and assembles config", async () => {
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  const fetchImpl = vi.fn(async () => buildResponse({ value: "1500" }));
  const config = await buildDefaultConfig(new Date("2026-05-28T15:00:00Z"), fetchImpl);
  expect(config.upperBoundId).toBe(1500);
});

it("buildDefaultConfig nowImpl returns the supplied now value", async () => {
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  const fetchImpl = vi.fn(async () => buildResponse({ value: "1500" }));
  const config = await buildDefaultConfig(new Date("2026-05-28T15:00:00Z"), fetchImpl);
  expect(config.nowImpl()).toStrictEqual(new Date("2026-05-28T15:00:00Z"));
});

it("buildDefaultConfig throws when b1-max-id endpoint returns non-ok", async () => {
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  const fetchImpl = vi.fn(async () => buildResponse({ error: "unauthorized" }, { status: 401 }));
  await expect(buildDefaultConfig(new Date(), fetchImpl)).rejects.toThrow();
});

it("buildDefaultConfig throws when b1-max-id value is null", async () => {
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  const fetchImpl = vi.fn(async () => buildResponse({ value: null }));
  await expect(buildDefaultConfig(new Date(), fetchImpl)).rejects.toThrow();
});

it("buildDefaultConfig throws when b1-max-id value is invalid number", async () => {
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  const fetchImpl = vi.fn(async () => buildResponse({ value: "not-a-number" }));
  await expect(buildDefaultConfig(new Date(), fetchImpl)).rejects.toThrow();
});

it("buildDefaultConfig throws when env var missing", async () => {
  delete process.env.REALTIME_ADMIN_TOKEN;
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  const fetchImpl = vi.fn(async () => buildResponse({ value: "1500" }));
  await expect(buildDefaultConfig(new Date(), fetchImpl)).rejects.toThrow();
});
