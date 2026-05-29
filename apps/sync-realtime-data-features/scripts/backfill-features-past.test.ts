// Run with: bun run --filter sync-realtime-data-features test
import { afterEach, beforeEach, expect, it, vi } from "vitest";

import {
  backfillFeaturesPast,
  type BackfillFeaturesPastConfig,
  buildDefaultConfig,
  computeColdStartDate,
  getJstHour,
  isWithinNightWindow,
  parseRaceKey,
  previousDate,
} from "./backfill-features-past";

const buildResponse = (status: number, body: unknown): Response =>
  new Response(JSON.stringify(body), {
    headers: { "content-type": "application/json" },
    status,
  });

const buildSleep = () => vi.fn().mockResolvedValue(undefined);

const buildNightNow = () => new Date("2026-05-29T16:00:00Z"); // 01:00 JST

const buildConfig = (
  fetchImpl: typeof fetch,
  overrides: Partial<BackfillFeaturesPastConfig> = {},
): BackfillFeaturesPastConfig => ({
  adminToken: "admin-token",
  b1FloorDays: 30,
  circuitPauseMs: 100,
  fetchImpl,
  internalToken: "internal-token",
  maxDaysPerRun: 7,
  newFeaturesWorkerUrl: "https://new",
  nowProvider: () => buildNightNow(),
  oldWorkerUrl: "https://old",
  perDaySleepMs: 1,
  perRaceSleepMs: 1,
  retryBackoffMaxMs: 60_000,
  retryBackoffStartMs: 2,
  retryLimit: 3,
  sleepImpl: buildSleep(),
  ...overrides,
});

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
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

it("parseRaceKey returns null on unknown source", () => {
  expect(parseRaceKey("ban-ei:2026:0529:83:01")).toBeNull();
});

it("parseRaceKey returns null on wrong shape", () => {
  expect(parseRaceKey("nar:2026:0529:30")).toBeNull();
});

it("previousDate moves YYYYMMDD back by one day across month boundary", () => {
  expect(previousDate("20260501")).toBe("20260430");
});

it("previousDate handles year boundary", () => {
  expect(previousDate("20260101")).toBe("20251231");
});

it("computeColdStartDate returns now minus b1FloorDays in UTC", () => {
  expect(computeColdStartDate(new Date("2026-05-29T00:00:00Z"), 30)).toBe("20260429");
});

it("getJstHour converts UTC instant to JST hour", () => {
  expect(getJstHour(new Date("2026-05-29T16:00:00Z"))).toBe(1);
});

it("isWithinNightWindow returns true for JST 01:00", () => {
  expect(isWithinNightWindow(new Date("2026-05-29T16:00:00Z"))).toBe(true);
});

it("isWithinNightWindow returns true for JST 23:30", () => {
  expect(isWithinNightWindow(new Date("2026-05-29T14:30:00Z"))).toBe(true);
});

it("isWithinNightWindow returns false for JST 05:00", () => {
  expect(isWithinNightWindow(new Date("2026-05-29T20:00:00Z"))).toBe(false);
});

it("isWithinNightWindow returns false for JST midday", () => {
  expect(isWithinNightWindow(new Date("2026-05-29T03:00:00Z"))).toBe(false);
});

it("backfillFeaturesPast resumes from KV value and walks one day older", async () => {
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.includes("/api/internal/migration-state?key=")) {
      return buildResponse(200, { key: "b3-last-seeded-date", value: "20260420" });
    }
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(200, { rows: [{ race_key: "nar:2026:0419:30:08" }] });
    }
    if (url.endsWith("/api/internal/recompute-and-build-parquet")) {
      return buildResponse(200, {
        builtAt: "now",
        r2Key: "features/by-race/2026/04/19/nar/30/08.parquet",
        raceKey: "nar:2026:0419:30:08",
        rowCount: 12,
      });
    }
    if (url.endsWith("/api/internal/migration-state")) {
      return buildResponse(200, { ok: true });
    }
    return buildResponse(404, {});
  });
  const config = buildConfig(fetchImpl as unknown as typeof fetch, { maxDaysPerRun: 1 });
  const result = await backfillFeaturesPast(config);
  expect(result.stoppedReason).toBe("max-days-reached");
  expect(result.totalDays).toBe(1);
  expect(result.totalRaces).toBe(1);
  expect(result.totalRows).toBe(12);
  expect(result.finalDate).toBe("20260419");
});

it("backfillFeaturesPast cold-starts from b1 floor minus one day when KV empty", async () => {
  const recomputeBodies: string[] = [];
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.includes("/api/internal/migration-state?key=")) {
      return buildResponse(200, { key: "b3-last-seeded-date", value: null });
    }
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(200, { rows: [{ race_key: "nar:2026:0429:30:08" }] });
    }
    if (url.endsWith("/api/internal/recompute-and-build-parquet")) {
      recomputeBodies.push(init?.body as string);
      return buildResponse(200, {
        builtAt: "now",
        r2Key: "k",
        raceKey: "nar:2026:0429:30:08",
        rowCount: 3,
      });
    }
    return buildResponse(200, { ok: true });
  });
  const config = buildConfig(fetchImpl as unknown as typeof fetch, { maxDaysPerRun: 1 });
  const result = await backfillFeaturesPast(config);
  expect(result.finalDate).toBe("20260429");
  expect(recomputeBodies[0]).toBe(
    '{"kaisaiNen":"2026","kaisaiTsukihi":"0429","keibajoCode":"30","raceBango":"08","raceKey":"nar:2026:0429:30:08","source":"nar"}',
  );
});

it("backfillFeaturesPast returns outside-night-window when starting outside the window", async () => {
  const fetchImpl = vi.fn(async () => buildResponse(200, { ok: true }));
  const config = buildConfig(fetchImpl as unknown as typeof fetch, {
    nowProvider: () => new Date("2026-05-29T20:00:00Z"), // 05:00 JST
  });
  const result = await backfillFeaturesPast(config);
  expect(result.stoppedReason).toBe("outside-night-window");
  expect(result.totalDays).toBe(0);
  expect(result.finalDate).toBeNull();
  expect(fetchImpl).not.toHaveBeenCalled();
});

it("backfillFeaturesPast stops mid-loop when JST hour crosses 05:00", async () => {
  const nowSequence = [
    new Date("2026-05-29T16:00:00Z"), // 01:00 JST (initial guard)
    new Date("2026-05-29T16:00:00Z"), // 01:00 JST (loop top day 1)
    new Date("2026-05-29T16:00:00Z"), // 01:00 JST (race 1 night check)
    new Date("2026-05-29T20:00:00Z"), // 05:00 JST (race 2 night check -> exit)
    new Date("2026-05-29T20:00:00Z"),
    new Date("2026-05-29T20:00:00Z"),
  ];
  let idx = 0;
  const nowProvider = () => {
    const next = nowSequence[idx] ?? new Date("2026-05-29T20:00:00Z");
    idx += 1;
    return next;
  };
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.includes("/api/internal/migration-state?key=")) {
      return buildResponse(200, { key: "b3-last-seeded-date", value: "20260420" });
    }
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(200, {
        rows: [{ race_key: "nar:2026:0419:30:01" }, { race_key: "nar:2026:0419:30:02" }],
      });
    }
    if (url.endsWith("/api/internal/recompute-and-build-parquet")) {
      return buildResponse(200, {
        builtAt: "n",
        r2Key: "k",
        raceKey: "nar:2026:0419:30:01",
        rowCount: 1,
      });
    }
    return buildResponse(200, { ok: true });
  });
  const config = buildConfig(fetchImpl as unknown as typeof fetch, {
    maxDaysPerRun: 5,
    nowProvider,
  });
  const result = await backfillFeaturesPast(config);
  expect(result.stoppedReason).toBe("outside-night-window");
  expect(result.totalRaces).toBe(1);
});

it("backfillFeaturesPast retries recompute with exponential backoff on 429 then succeeds", async () => {
  const attempts: number[] = [];
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.includes("/api/internal/migration-state?key=")) {
      return buildResponse(200, { key: "b3-last-seeded-date", value: "20260420" });
    }
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(200, { rows: [{ race_key: "nar:2026:0419:30:01" }] });
    }
    if (url.endsWith("/api/internal/recompute-and-build-parquet")) {
      attempts.push(1);
      if (attempts.length === 1) {
        return buildResponse(429, { error: "saturated" });
      }
      return buildResponse(200, {
        builtAt: "n",
        r2Key: "k",
        raceKey: "nar:2026:0419:30:01",
        rowCount: 2,
      });
    }
    return buildResponse(200, { ok: true });
  });
  const sleepImpl = buildSleep();
  const config = buildConfig(fetchImpl as unknown as typeof fetch, {
    maxDaysPerRun: 1,
    retryBackoffStartMs: 2,
    sleepImpl,
  });
  const result = await backfillFeaturesPast(config);
  expect(result.totalRows).toBe(2);
  expect(attempts.length).toBe(2);
  expect(sleepImpl).toHaveBeenCalledWith(2);
});

it("backfillFeaturesPast caps exponential backoff at retryBackoffMaxMs", async () => {
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.includes("/api/internal/migration-state?key=")) {
      return buildResponse(200, { key: "b3-last-seeded-date", value: "20260420" });
    }
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(503, { error: "down" });
    }
    return buildResponse(200, { ok: true });
  });
  const sleepImpl = buildSleep();
  const config = buildConfig(fetchImpl as unknown as typeof fetch, {
    maxDaysPerRun: 1,
    retryBackoffMaxMs: 5,
    retryBackoffStartMs: 4,
    retryLimit: 3,
    sleepImpl,
  });
  await expect(backfillFeaturesPast(config)).rejects.toThrowError(/list-race-keys/u);
  expect(sleepImpl).toHaveBeenCalledWith(4);
  expect(sleepImpl).toHaveBeenCalledWith(5);
});

it("backfillFeaturesPast opens circuit and throws after 3 consecutive recompute failures", async () => {
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.includes("/api/internal/migration-state?key=")) {
      return buildResponse(200, { key: "b3-last-seeded-date", value: "20260420" });
    }
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(200, {
        rows: [
          { race_key: "nar:2026:0419:30:01" },
          { race_key: "nar:2026:0419:30:02" },
          { race_key: "nar:2026:0419:30:03" },
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
    maxDaysPerRun: 1,
    retryLimit: 3,
    sleepImpl,
  });
  await expect(backfillFeaturesPast(config)).rejects.toThrowError(/500/u);
  expect(sleepImpl).toHaveBeenCalledWith(100);
});

it("backfillFeaturesPast honors maxDaysPerRun and stops with max-days-reached", async () => {
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.includes("/api/internal/migration-state?key=")) {
      return buildResponse(200, { key: "b3-last-seeded-date", value: "20260420" });
    }
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(200, { rows: [] });
    }
    return buildResponse(200, { ok: true });
  });
  const config = buildConfig(fetchImpl as unknown as typeof fetch, { maxDaysPerRun: 3 });
  const result = await backfillFeaturesPast(config);
  expect(result.stoppedReason).toBe("max-days-reached");
  expect(result.totalDays).toBe(3);
  expect(result.finalDate).toBe("20260417");
});

it("backfillFeaturesPast invokes old-worker list endpoint with authorization", async () => {
  const calls: { url: string; init?: RequestInit }[] = [];
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    calls.push({ init, url });
    if (url.includes("/api/internal/migration-state?key=")) {
      return buildResponse(200, { key: "b3-last-seeded-date", value: "20260420" });
    }
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(200, { rows: [] });
    }
    return buildResponse(200, { ok: true });
  });
  const config = buildConfig(fetchImpl as unknown as typeof fetch, { maxDaysPerRun: 1 });
  await backfillFeaturesPast(config);
  const listCall = calls.find((c) =>
    c.url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive"),
  );
  expect(listCall).toBeDefined();
  expect(listCall!.url).toBe("https://old/api/internal/list-race-keys-by-date-from-hyperdrive");
  expect(listCall!.init!.body).toBe('{"kaisaiNen":"2026","kaisaiTsukihi":"0419"}');
});

it("backfillFeaturesPast persists progress after each day", async () => {
  const stateBodies: string[] = [];
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.includes("/api/internal/migration-state?key=")) {
      return buildResponse(200, { key: "b3-last-seeded-date", value: "20260420" });
    }
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(200, { rows: [] });
    }
    if (
      url.endsWith("/api/internal/migration-state") &&
      (init?.method ?? "GET").toUpperCase() === "POST"
    ) {
      stateBodies.push(init?.body as string);
      return buildResponse(200, { ok: true });
    }
    return buildResponse(200, { ok: true });
  });
  const config = buildConfig(fetchImpl as unknown as typeof fetch, { maxDaysPerRun: 2 });
  await backfillFeaturesPast(config);
  expect(stateBodies[0]).toBe('{"key":"b3-last-seeded-date","value":"20260419"}');
  expect(stateBodies[1]).toBe('{"key":"b3-last-seeded-date","value":"20260418"}');
});

it("backfillFeaturesPast invokes new-worker recompute with correct bundle", async () => {
  const recomputeBodies: string[] = [];
  const recomputeUrls: string[] = [];
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.includes("/api/internal/migration-state?key=")) {
      return buildResponse(200, { key: "b3-last-seeded-date", value: "20260420" });
    }
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(200, { rows: [{ race_key: "jra:2026:0419:08:01" }] });
    }
    if (url.endsWith("/api/internal/recompute-and-build-parquet")) {
      recomputeBodies.push(init?.body as string);
      recomputeUrls.push(url);
      return buildResponse(200, {
        builtAt: "n",
        r2Key: "k",
        raceKey: "jra:2026:0419:08:01",
        rowCount: 7,
      });
    }
    return buildResponse(200, { ok: true });
  });
  const config = buildConfig(fetchImpl as unknown as typeof fetch, { maxDaysPerRun: 1 });
  await backfillFeaturesPast(config);
  expect(recomputeUrls[0]).toBe("https://new/api/internal/recompute-and-build-parquet");
  expect(recomputeBodies[0]).toBe(
    '{"kaisaiNen":"2026","kaisaiTsukihi":"0419","keibajoCode":"08","raceBango":"01","raceKey":"jra:2026:0419:08:01","source":"jra"}',
  );
});

it("backfillFeaturesPast retries list-race-keys on 429 then succeeds", async () => {
  const listAttempts: number[] = [];
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.includes("/api/internal/migration-state?key=")) {
      return buildResponse(200, { key: "b3-last-seeded-date", value: "20260420" });
    }
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      listAttempts.push(1);
      if (listAttempts.length === 1) {
        return buildResponse(429, { error: "saturated" });
      }
      return buildResponse(200, { rows: [] });
    }
    return buildResponse(200, { ok: true });
  });
  const sleepImpl = buildSleep();
  const config = buildConfig(fetchImpl as unknown as typeof fetch, {
    maxDaysPerRun: 1,
    sleepImpl,
  });
  const result = await backfillFeaturesPast(config);
  expect(result.totalRaces).toBe(0);
  expect(listAttempts.length).toBe(2);
});

it("backfillFeaturesPast skips malformed race_keys without counting rows", async () => {
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.includes("/api/internal/migration-state?key=")) {
      return buildResponse(200, { key: "b3-last-seeded-date", value: "20260420" });
    }
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(200, { rows: [{ race_key: "not-valid" }] });
    }
    return buildResponse(200, { ok: true });
  });
  const config = buildConfig(fetchImpl as unknown as typeof fetch, { maxDaysPerRun: 1 });
  const result = await backfillFeaturesPast(config);
  expect(result.totalRaces).toBe(1);
  expect(result.totalRows).toBe(0);
});

it("backfillFeaturesPast throws when loadResumeDate endpoint fails", async () => {
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.includes("/api/internal/migration-state?key=")) {
      return buildResponse(500, { error: "kv-down" });
    }
    return buildResponse(200, { ok: true });
  });
  const config = buildConfig(fetchImpl as unknown as typeof fetch);
  await expect(backfillFeaturesPast(config)).rejects.toThrowError(/load-resume-date/u);
});

it("backfillFeaturesPast sleeps perDaySleepMs between days", async () => {
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.includes("/api/internal/migration-state?key=")) {
      return buildResponse(200, { key: "b3-last-seeded-date", value: "20260420" });
    }
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(200, { rows: [] });
    }
    return buildResponse(200, { ok: true });
  });
  const sleepImpl = buildSleep();
  const config = buildConfig(fetchImpl as unknown as typeof fetch, {
    maxDaysPerRun: 2,
    perDaySleepMs: 5000,
    sleepImpl,
  });
  await backfillFeaturesPast(config);
  expect(sleepImpl).toHaveBeenCalledWith(5000);
});

it("backfillFeaturesPast sleeps perRaceSleepMs between races", async () => {
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.includes("/api/internal/migration-state?key=")) {
      return buildResponse(200, { key: "b3-last-seeded-date", value: "20260420" });
    }
    if (url.endsWith("/api/internal/list-race-keys-by-date-from-hyperdrive")) {
      return buildResponse(200, {
        rows: [{ race_key: "nar:2026:0419:30:01" }, { race_key: "nar:2026:0419:30:02" }],
      });
    }
    if (url.endsWith("/api/internal/recompute-and-build-parquet")) {
      return buildResponse(200, { builtAt: "n", r2Key: "k", raceKey: "r", rowCount: 1 });
    }
    return buildResponse(200, { ok: true });
  });
  const sleepImpl = buildSleep();
  const config = buildConfig(fetchImpl as unknown as typeof fetch, {
    maxDaysPerRun: 1,
    perRaceSleepMs: 500,
    sleepImpl,
  });
  await backfillFeaturesPast(config);
  expect(sleepImpl).toHaveBeenCalledWith(500);
});

it("buildDefaultConfig throws when REALTIME_ADMIN_TOKEN missing", () => {
  vi.stubEnv("REALTIME_ADMIN_TOKEN", "");
  vi.stubEnv("NEW_FEATURES_WORKER_URL", "https://new");
  vi.stubEnv("OLD_WORKER_URL", "https://old");
  vi.stubEnv("FEATURES_INTERNAL_TOKEN", "tok");
  expect(() => buildDefaultConfig(() => new Date())).toThrowError(/REALTIME_ADMIN_TOKEN/u);
  vi.unstubAllEnvs();
});

it("buildDefaultConfig falls back to PC_KEIBA_VIEWER_INTERNAL_TOKEN when FEATURES_INTERNAL_TOKEN missing", () => {
  vi.stubEnv("REALTIME_ADMIN_TOKEN", "admin");
  vi.stubEnv("NEW_FEATURES_WORKER_URL", "https://new");
  vi.stubEnv("OLD_WORKER_URL", "https://old");
  vi.stubEnv("FEATURES_INTERNAL_TOKEN", "");
  vi.stubEnv("PC_KEIBA_VIEWER_INTERNAL_TOKEN", "pc-token");
  const config = buildDefaultConfig(() => new Date());
  expect(config.internalToken).toBe("pc-token");
  vi.unstubAllEnvs();
});

it("buildDefaultConfig prefers explicit FEATURES_INTERNAL_TOKEN", () => {
  vi.stubEnv("REALTIME_ADMIN_TOKEN", "admin");
  vi.stubEnv("NEW_FEATURES_WORKER_URL", "https://new");
  vi.stubEnv("OLD_WORKER_URL", "https://old");
  vi.stubEnv("FEATURES_INTERNAL_TOKEN", "features-token");
  vi.stubEnv("PC_KEIBA_VIEWER_INTERNAL_TOKEN", "pc-token");
  const config = buildDefaultConfig(() => new Date());
  expect(config.internalToken).toBe("features-token");
  vi.unstubAllEnvs();
});

it("buildDefaultConfig honors BACKFILL_MAX_DAYS env var when valid", () => {
  vi.stubEnv("REALTIME_ADMIN_TOKEN", "admin");
  vi.stubEnv("NEW_FEATURES_WORKER_URL", "https://new");
  vi.stubEnv("OLD_WORKER_URL", "https://old");
  vi.stubEnv("FEATURES_INTERNAL_TOKEN", "tok");
  vi.stubEnv("BACKFILL_MAX_DAYS", "3");
  expect(buildDefaultConfig(() => new Date()).maxDaysPerRun).toBe(3);
  vi.unstubAllEnvs();
});

it("buildDefaultConfig falls back to default maxDaysPerRun when BACKFILL_MAX_DAYS non-numeric", () => {
  vi.stubEnv("REALTIME_ADMIN_TOKEN", "admin");
  vi.stubEnv("NEW_FEATURES_WORKER_URL", "https://new");
  vi.stubEnv("OLD_WORKER_URL", "https://old");
  vi.stubEnv("FEATURES_INTERNAL_TOKEN", "tok");
  vi.stubEnv("BACKFILL_MAX_DAYS", "x");
  expect(buildDefaultConfig(() => new Date()).maxDaysPerRun).toBe(7);
  vi.unstubAllEnvs();
});

it("buildDefaultConfig falls back to default maxDaysPerRun when BACKFILL_MAX_DAYS empty", () => {
  vi.stubEnv("REALTIME_ADMIN_TOKEN", "admin");
  vi.stubEnv("NEW_FEATURES_WORKER_URL", "https://new");
  vi.stubEnv("OLD_WORKER_URL", "https://old");
  vi.stubEnv("FEATURES_INTERNAL_TOKEN", "tok");
  vi.stubEnv("BACKFILL_MAX_DAYS", "");
  expect(buildDefaultConfig(() => new Date()).maxDaysPerRun).toBe(7);
  vi.unstubAllEnvs();
});

it("buildDefaultConfig honors B1_FLOOR_DAYS env var", () => {
  vi.stubEnv("REALTIME_ADMIN_TOKEN", "admin");
  vi.stubEnv("NEW_FEATURES_WORKER_URL", "https://new");
  vi.stubEnv("OLD_WORKER_URL", "https://old");
  vi.stubEnv("FEATURES_INTERNAL_TOKEN", "tok");
  vi.stubEnv("B1_FLOOR_DAYS", "45");
  expect(buildDefaultConfig(() => new Date()).b1FloorDays).toBe(45);
  vi.unstubAllEnvs();
});

it("buildDefaultConfig falls back to default b1FloorDays when env missing", () => {
  vi.stubEnv("REALTIME_ADMIN_TOKEN", "admin");
  vi.stubEnv("NEW_FEATURES_WORKER_URL", "https://new");
  vi.stubEnv("OLD_WORKER_URL", "https://old");
  vi.stubEnv("FEATURES_INTERNAL_TOKEN", "tok");
  expect(buildDefaultConfig(() => new Date()).b1FloorDays).toBe(30);
  vi.unstubAllEnvs();
});
