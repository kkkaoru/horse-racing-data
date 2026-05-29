// Run with: bun run --filter sync-realtime-data-features test
import { afterEach, beforeEach, expect, it, vi } from "vitest";

import {
  buildDefaultConfig,
  buildPastDateList,
  parseRaceKey,
  seedFeaturesRecompute,
  type SeedFeaturesRecomputeConfig,
} from "./seed-features-recompute";

const buildResponse = (status: number, body: unknown): Response =>
  new Response(JSON.stringify(body), {
    headers: { "content-type": "application/json" },
    status,
  });

const buildSleep = () => vi.fn().mockResolvedValue(undefined);

const buildConfig = (
  fetchImpl: typeof fetch,
  overrides: Partial<SeedFeaturesRecomputeConfig> = {},
): SeedFeaturesRecomputeConfig => ({
  adminToken: "admin-token",
  circuitPauseMs: 100,
  fetchImpl,
  internalToken: "internal-token",
  newFeaturesWorkerUrl: "https://new",
  now: new Date("2026-05-29T00:00:00Z"),
  oldWorkerUrl: "https://old",
  perDaySleepMs: 1,
  perRaceSleepMs: 1,
  retryBackoffMs: 1,
  retryLimit: 3,
  seedDays: 1,
  sleepImpl: buildSleep(),
  ...overrides,
});

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("buildPastDateList returns descending YYYYMMDD list for N days", () => {
  const list = buildPastDateList(new Date("2026-05-29T12:00:00Z"), 3);
  expect(list).toStrictEqual(["20260529", "20260528", "20260527"]);
});

it("parseRaceKey parses a 5-part nar race_key", () => {
  expect(parseRaceKey("nar:2026:0529:30:08")).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
    source: "nar",
  });
});

it("parseRaceKey parses a 5-part jra race_key", () => {
  expect(parseRaceKey("jra:2026:0529:08:01")).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "08",
    raceBango: "01",
    source: "jra",
  });
});

it("parseRaceKey returns null when source is unsupported", () => {
  expect(parseRaceKey("ban-ei:2026:0529:83:01")).toBeNull();
});

it("parseRaceKey returns null when shape mismatches", () => {
  expect(parseRaceKey("nar:2026:0529:30")).toBeNull();
});

it("seedFeaturesRecompute builds every race returned by list endpoint", async () => {
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(200, { rows: [{ race_key: "nar:2026:0529:30:08" }] });
    }
    if (url.endsWith("/api/internal/recompute-and-build-parquet")) {
      return buildResponse(200, {
        builtAt: "now",
        r2Key: "features/by-race/2026/05/29/nar/30/08.parquet",
        raceKey: "nar:2026:0529:30:08",
        rowCount: 14,
      });
    }
    if (url.endsWith("/api/internal/migration-state")) {
      return buildResponse(200, { ok: true });
    }
    return buildResponse(404, {});
  });
  const config = buildConfig(fetchImpl as unknown as typeof fetch);
  const result = await seedFeaturesRecompute(config);
  expect(result.totalRaces).toBe(1);
  expect(result.totalBuilt).toBe(1);
  expect(result.totalFailed).toBe(0);
  expect(result.lastRaceKey).toBe("nar:2026:0529:30:08");
});

it("seedFeaturesRecompute saves migration state after a successful build", async () => {
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(200, { rows: [{ race_key: "nar:2026:0529:30:08" }] });
    }
    if (url.endsWith("/api/internal/recompute-and-build-parquet")) {
      return buildResponse(200, {
        builtAt: "now",
        r2Key: "k",
        raceKey: "nar:2026:0529:30:08",
        rowCount: 1,
      });
    }
    return buildResponse(200, { ok: true });
  });
  const config = buildConfig(fetchImpl as unknown as typeof fetch);
  await seedFeaturesRecompute(config);
  const extractUrl = (input: RequestInfo | URL): string =>
    typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
  const stateCall = fetchImpl.mock.calls.find((call) =>
    extractUrl(call[0]).endsWith("/api/internal/migration-state"),
  );
  expect(stateCall).toBeDefined();
  expect(stateCall![1]!.body).toBe(
    '{"key":"b1-last-recomputed-race-key","value":"nar:2026:0529:30:08"}',
  );
});

it("seedFeaturesRecompute opens circuit and throws after consecutive recompute failures", async () => {
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(200, {
        rows: [
          { race_key: "nar:2026:0529:30:01" },
          { race_key: "nar:2026:0529:30:02" },
          { race_key: "nar:2026:0529:30:03" },
        ],
      });
    }
    if (url.endsWith("/api/internal/recompute-and-build-parquet")) {
      return buildResponse(500, { error: "internal" });
    }
    return buildResponse(200, { ok: true });
  });
  const sleepImpl = buildSleep();
  const config = buildConfig(fetchImpl as unknown as typeof fetch, {
    retryLimit: 3,
    sleepImpl,
  });
  await expect(seedFeaturesRecompute(config)).rejects.toThrowError(/500/u);
  expect(sleepImpl).toHaveBeenCalledWith(100);
});

it("seedFeaturesRecompute retries recompute on transient 429 then succeeds", async () => {
  const attempts: number[] = [];
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(200, { rows: [{ race_key: "nar:2026:0529:30:08" }] });
    }
    if (url.endsWith("/api/internal/recompute-and-build-parquet")) {
      attempts.push(1);
      if (attempts.length === 1) {
        return buildResponse(429, { error: "saturated" });
      }
      return buildResponse(200, {
        builtAt: "n",
        r2Key: "k",
        raceKey: "nar:2026:0529:30:08",
        rowCount: 1,
      });
    }
    return buildResponse(200, { ok: true });
  });
  const sleepImpl = buildSleep();
  const config = buildConfig(fetchImpl as unknown as typeof fetch, { sleepImpl });
  const result = await seedFeaturesRecompute(config);
  expect(result.totalBuilt).toBe(1);
  expect(attempts.length).toBe(2);
  expect(sleepImpl).toHaveBeenCalledWith(1);
});

it("seedFeaturesRecompute retries list-race-keys on transient 429 then succeeds", async () => {
  const listAttempts: number[] = [];
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      listAttempts.push(1);
      if (listAttempts.length === 1) {
        return buildResponse(429, { error: "saturated" });
      }
      return buildResponse(200, { rows: [] });
    }
    return buildResponse(200, { ok: true });
  });
  const config = buildConfig(fetchImpl as unknown as typeof fetch);
  const result = await seedFeaturesRecompute(config);
  expect(result.totalRaces).toBe(0);
  expect(listAttempts.length).toBe(2);
});

it("seedFeaturesRecompute throws when list-race-keys exhausts retries", async () => {
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(503, { error: "down" });
    }
    return buildResponse(200, { ok: true });
  });
  const config = buildConfig(fetchImpl as unknown as typeof fetch, { retryLimit: 2 });
  await expect(seedFeaturesRecompute(config)).rejects.toThrowError(/list-race-keys/u);
});

it("seedFeaturesRecompute counts malformed race_keys as failed without crashing", async () => {
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(200, { rows: [{ race_key: "not-a-valid-race-key" }] });
    }
    return buildResponse(200, { ok: true });
  });
  const config = buildConfig(fetchImpl as unknown as typeof fetch);
  const result = await seedFeaturesRecompute(config);
  expect(result.totalRaces).toBe(1);
  expect(result.totalBuilt).toBe(0);
  expect(result.totalFailed).toBe(1);
});

it("seedFeaturesRecompute sleeps perRaceSleepMs between races", async () => {
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(200, {
        rows: [{ race_key: "nar:2026:0529:30:01" }, { race_key: "nar:2026:0529:30:02" }],
      });
    }
    if (url.endsWith("/api/internal/recompute-and-build-parquet")) {
      return buildResponse(200, { builtAt: "n", r2Key: "k", raceKey: "r", rowCount: 1 });
    }
    return buildResponse(200, { ok: true });
  });
  const sleepImpl = buildSleep();
  const config = buildConfig(fetchImpl as unknown as typeof fetch, {
    perRaceSleepMs: 500,
    sleepImpl,
  });
  await seedFeaturesRecompute(config);
  expect(sleepImpl).toHaveBeenCalledWith(500);
});

it("seedFeaturesRecompute sleeps perDaySleepMs between days", async () => {
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(200, { rows: [] });
    }
    return buildResponse(200, { ok: true });
  });
  const sleepImpl = buildSleep();
  const config = buildConfig(fetchImpl as unknown as typeof fetch, {
    perDaySleepMs: 5000,
    seedDays: 2,
    sleepImpl,
  });
  await seedFeaturesRecompute(config);
  expect(sleepImpl).toHaveBeenCalledWith(5000);
});

it("buildPastDateList returns a single date when seedDays is 1", () => {
  expect(buildPastDateList(new Date("2026-05-29T00:00:00Z"), 1)).toStrictEqual(["20260529"]);
});

it("buildDefaultConfig throws when REALTIME_ADMIN_TOKEN missing", () => {
  vi.stubEnv("REALTIME_ADMIN_TOKEN", "");
  vi.stubEnv("NEW_FEATURES_WORKER_URL", "https://new");
  vi.stubEnv("OLD_WORKER_URL", "https://old");
  vi.stubEnv("FEATURES_INTERNAL_TOKEN", "tok");
  expect(() => buildDefaultConfig(new Date())).toThrowError(/REALTIME_ADMIN_TOKEN/u);
  vi.unstubAllEnvs();
});

it("buildDefaultConfig falls back to PC_KEIBA_VIEWER_INTERNAL_TOKEN when explicit missing", () => {
  vi.stubEnv("REALTIME_ADMIN_TOKEN", "admin");
  vi.stubEnv("NEW_FEATURES_WORKER_URL", "https://new");
  vi.stubEnv("OLD_WORKER_URL", "https://old");
  vi.stubEnv("FEATURES_INTERNAL_TOKEN", "");
  vi.stubEnv("PC_KEIBA_VIEWER_INTERNAL_TOKEN", "pc-token");
  const config = buildDefaultConfig(new Date());
  expect(config.internalToken).toBe("pc-token");
  vi.unstubAllEnvs();
});

it("buildDefaultConfig prefers explicit FEATURES_INTERNAL_TOKEN over PC_KEIBA_VIEWER_INTERNAL_TOKEN", () => {
  vi.stubEnv("REALTIME_ADMIN_TOKEN", "admin");
  vi.stubEnv("NEW_FEATURES_WORKER_URL", "https://new");
  vi.stubEnv("OLD_WORKER_URL", "https://old");
  vi.stubEnv("FEATURES_INTERNAL_TOKEN", "features-token");
  vi.stubEnv("PC_KEIBA_VIEWER_INTERNAL_TOKEN", "pc-token");
  const config = buildDefaultConfig(new Date());
  expect(config.internalToken).toBe("features-token");
  vi.unstubAllEnvs();
});

it("buildDefaultConfig uses SEED_DAYS env var when valid", () => {
  vi.stubEnv("REALTIME_ADMIN_TOKEN", "admin");
  vi.stubEnv("NEW_FEATURES_WORKER_URL", "https://new");
  vi.stubEnv("OLD_WORKER_URL", "https://old");
  vi.stubEnv("FEATURES_INTERNAL_TOKEN", "tok");
  vi.stubEnv("SEED_DAYS", "7");
  expect(buildDefaultConfig(new Date()).seedDays).toBe(7);
  vi.unstubAllEnvs();
});

it("buildDefaultConfig falls back to default seedDays when SEED_DAYS is non-numeric", () => {
  vi.stubEnv("REALTIME_ADMIN_TOKEN", "admin");
  vi.stubEnv("NEW_FEATURES_WORKER_URL", "https://new");
  vi.stubEnv("OLD_WORKER_URL", "https://old");
  vi.stubEnv("FEATURES_INTERNAL_TOKEN", "tok");
  vi.stubEnv("SEED_DAYS", "x");
  expect(buildDefaultConfig(new Date()).seedDays).toBe(30);
  vi.unstubAllEnvs();
});

it("buildDefaultConfig falls back to default seedDays when SEED_DAYS is empty", () => {
  vi.stubEnv("REALTIME_ADMIN_TOKEN", "admin");
  vi.stubEnv("NEW_FEATURES_WORKER_URL", "https://new");
  vi.stubEnv("OLD_WORKER_URL", "https://old");
  vi.stubEnv("FEATURES_INTERNAL_TOKEN", "tok");
  vi.stubEnv("SEED_DAYS", "");
  expect(buildDefaultConfig(new Date()).seedDays).toBe(30);
  vi.unstubAllEnvs();
});
