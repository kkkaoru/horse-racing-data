// Run with bun.
import { expect, it, vi } from "vitest";

import {
  buildDefaultConfig,
  exportOldOddsToR2Final,
  type ExportChunkResponse,
  type ExportFinalBackupConfig,
} from "./export-old-odds-to-r2-final";

const buildConfig = (
  overrides: Partial<ExportFinalBackupConfig> = {},
): ExportFinalBackupConfig => ({
  adminToken: "admin",
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
  upperBoundId: 100,
  ...overrides,
});

const buildResponse = (body: unknown, init?: ResponseInit): Response =>
  new Response(JSON.stringify(body), {
    headers: { "content-type": "application/json" },
    status: init?.status ?? 200,
  });

it("stops immediately when resume id already meets upper bound", async () => {
  const fetchImpl = vi.fn(async () => buildResponse({ value: "150" }));
  const config = buildConfig({ fetchImpl, upperBoundId: 100 });
  const result = await exportOldOddsToR2Final(config);
  expect(result.stoppedReason).toBe("upper-bound-reached");
  expect(result.totalRows).toBe(0);
});

it("stops when fetched chunk has empty rows", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValueOnce(buildResponse({ done: true, next_since_id: 0, rows: [] }));
  const config = buildConfig({ fetchImpl });
  const result = await exportOldOddsToR2Final(config);
  expect(result.stoppedReason).toBe("completed");
});

it("forwards rows to r2-archive and saves progress", async () => {
  const chunk: ExportChunkResponse = {
    done: false,
    next_since_id: 50,
    rows: [
      {
        average_odds: null,
        combination: "01",
        fetched_at: "2026-05-20T10:00:00+09:00",
        id: 50,
        max_odds: null,
        min_odds: null,
        odds: 2.5,
        odds_type: "tansho",
        race_key: "nar:20260520:42:01",
        rank: 1,
      },
    ],
  };
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValueOnce(buildResponse(chunk))
    .mockResolvedValueOnce(buildResponse({ groups: 1, rows: 1 }))
    .mockResolvedValueOnce(buildResponse({ ok: true }))
    .mockResolvedValueOnce(buildResponse({ done: true, next_since_id: 50, rows: [] }));
  const config = buildConfig({ fetchImpl });
  const result = await exportOldOddsToR2Final(config);
  expect(result.totalRows).toBe(1);
  expect(result.totalGroups).toBe(1);
});

it("returns completed when chunk reports done after archive", async () => {
  const chunk: ExportChunkResponse = {
    done: true,
    next_since_id: 60,
    rows: [
      {
        average_odds: null,
        combination: "01",
        fetched_at: "2026-05-20T10:00:00+09:00",
        id: 60,
        max_odds: null,
        min_odds: null,
        odds: 2.5,
        odds_type: "tansho",
        race_key: "nar:20260520:42:01",
        rank: 1,
      },
    ],
  };
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: null }))
    .mockResolvedValueOnce(buildResponse(chunk))
    .mockResolvedValueOnce(buildResponse({ groups: 1, rows: 1 }))
    .mockResolvedValueOnce(buildResponse({ ok: true }));
  const config = buildConfig({ fetchImpl });
  const result = await exportOldOddsToR2Final(config);
  expect(result.stoppedReason).toBe("completed");
  expect(result.finalSinceId).toBe(60);
});

it("treats non-ok resume response as zero", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ error: "missing" }, { status: 404 }))
    .mockResolvedValueOnce(buildResponse({ done: true, next_since_id: 0, rows: [] }));
  const config = buildConfig({ fetchImpl });
  const result = await exportOldOddsToR2Final(config);
  expect(result.totalRows).toBe(0);
});

it("treats non-numeric resume value as zero", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "garbage" }))
    .mockResolvedValueOnce(buildResponse({ done: true, next_since_id: 0, rows: [] }));
  const config = buildConfig({ fetchImpl });
  const result = await exportOldOddsToR2Final(config);
  expect(result.totalRows).toBe(0);
});

it("filters rows above upper bound and reports upper-bound-reached", async () => {
  const chunk: ExportChunkResponse = {
    done: false,
    next_since_id: 200,
    rows: [
      {
        average_odds: null,
        combination: "01",
        fetched_at: "2026-05-20T10:00:00+09:00",
        id: 150,
        max_odds: null,
        min_odds: null,
        odds: 2.5,
        odds_type: "tansho",
        race_key: "nar:20260520:42:01",
        rank: 1,
      },
    ],
  };
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValueOnce(buildResponse(chunk));
  const config = buildConfig({ fetchImpl, upperBoundId: 100 });
  const result = await exportOldOddsToR2Final(config);
  expect(result.stoppedReason).toBe("upper-bound-reached");
});

it("archives rows that pass the upper-bound filter", async () => {
  const chunk: ExportChunkResponse = {
    done: false,
    next_since_id: 200,
    rows: [
      {
        average_odds: null,
        combination: "01",
        fetched_at: "2026-05-20T10:00:00+09:00",
        id: 50,
        max_odds: null,
        min_odds: null,
        odds: 2.5,
        odds_type: "tansho",
        race_key: "nar:20260520:42:01",
        rank: 1,
      },
      {
        average_odds: null,
        combination: "02",
        fetched_at: "2026-05-20T10:00:00+09:00",
        id: 150,
        max_odds: null,
        min_odds: null,
        odds: 5.0,
        odds_type: "tansho",
        race_key: "nar:20260520:42:01",
        rank: 2,
      },
    ],
  };
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValueOnce(buildResponse(chunk))
    .mockResolvedValueOnce(buildResponse({ groups: 1, rows: 1 }))
    .mockResolvedValueOnce(buildResponse({ ok: true }))
    .mockResolvedValueOnce(buildResponse({ done: false, next_since_id: 50, rows: [] }));
  const config = buildConfig({ fetchImpl, upperBoundId: 100 });
  const result = await exportOldOddsToR2Final(config);
  expect(result.totalRows).toBe(1);
});

it("retries on saturation and surfaces error after retry limit", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValue(buildResponse({ error: "saturated" }, { status: 429 }));
  const config = buildConfig({ fetchImpl, retryLimit: 3 });
  await expect(exportOldOddsToR2Final(config)).rejects.toThrow();
});

it("throws when r2-archive-rows returns non-ok", async () => {
  const chunk: ExportChunkResponse = {
    done: false,
    next_since_id: 50,
    rows: [
      {
        average_odds: null,
        combination: "01",
        fetched_at: "2026-05-20T10:00:00+09:00",
        id: 50,
        max_odds: null,
        min_odds: null,
        odds: 2.5,
        odds_type: "tansho",
        race_key: "nar:20260520:42:01",
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
  await expect(exportOldOddsToR2Final(config)).rejects.toThrow();
});

it("throws when export-odds-chunk returns non-429 non-ok status", async () => {
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(buildResponse({ value: "0" }))
    .mockResolvedValue(buildResponse({ error: "forbidden" }, { status: 403 }));
  const config = buildConfig({ fetchImpl, retryLimit: 1 });
  await expect(exportOldOddsToR2Final(config)).rejects.toThrow();
});

it("throws when retryLimit is zero", async () => {
  const fetchImpl = vi.fn<typeof fetch>().mockResolvedValueOnce(buildResponse({ value: "0" }));
  const config = buildConfig({ fetchImpl, retryLimit: 0 });
  await expect(exportOldOddsToR2Final(config)).rejects.toThrow();
});

it("buildDefaultConfig reads env vars and fetches b1-max-id", async () => {
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  const fetchImpl = vi.fn(async () => buildResponse({ value: "1500" }));
  const config = await buildDefaultConfig(fetchImpl);
  expect(config.upperBoundId).toBe(1500);
});

it("buildDefaultConfig throws when b1-max-id endpoint returns non-ok", async () => {
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  const fetchImpl = vi.fn(async () => buildResponse({ error: "x" }, { status: 401 }));
  await expect(buildDefaultConfig(fetchImpl)).rejects.toThrow();
});

it("buildDefaultConfig throws when b1-max-id value is null", async () => {
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  const fetchImpl = vi.fn(async () => buildResponse({ value: null }));
  await expect(buildDefaultConfig(fetchImpl)).rejects.toThrow();
});

it("buildDefaultConfig throws when b1-max-id value is not a positive integer", async () => {
  process.env.REALTIME_ADMIN_TOKEN = "admin";
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  const fetchImpl = vi.fn(async () => buildResponse({ value: "not-a-number" }));
  await expect(buildDefaultConfig(fetchImpl)).rejects.toThrow();
});

it("buildDefaultConfig throws when an env var is missing", async () => {
  delete process.env.REALTIME_ADMIN_TOKEN;
  process.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN = "internal";
  process.env.NEW_WORKER_URL = "https://new.example.com";
  process.env.OLD_WORKER_URL = "https://old.example.com";
  const fetchImpl = vi.fn(async () => buildResponse({ value: "1500" }));
  await expect(buildDefaultConfig(fetchImpl)).rejects.toThrow();
});
