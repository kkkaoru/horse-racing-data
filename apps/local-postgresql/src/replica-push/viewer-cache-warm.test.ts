// Run via bun (vitest).
import { describe, expect, it, vi } from "vitest";

import {
  buildCacheWarmRequest,
  computeTomorrowJstDate,
  fireCacheWarmEndpoint,
  resolveViewerCacheWarmEnvironment,
  warmViewerCachesForTomorrowJst,
} from "./viewer-cache-warm";

interface FakeResponseInit {
  ok: boolean;
  status: number;
  jsonValue: unknown;
}

function createFakeResponse(init: FakeResponseInit): Response {
  return {
    ok: init.ok,
    status: init.status,
    json: async () => init.jsonValue,
  } as unknown as Response;
}

describe("viewer-cache-warm", () => {
  it("computes tomorrow JST date at UTC noon", () => {
    const isoDate = computeTomorrowJstDate({ now: new Date("2026-06-19T12:00:00.000Z") });
    expect(isoDate).toStrictEqual("2026-06-20");
  });

  it("computes tomorrow JST date when UTC time wraps to next JST day", () => {
    const isoDate = computeTomorrowJstDate({ now: new Date("2026-06-19T15:30:00.000Z") });
    expect(isoDate).toStrictEqual("2026-06-21");
  });

  it("computes tomorrow JST date during late UTC evening crossing month boundary", () => {
    const isoDate = computeTomorrowJstDate({ now: new Date("2026-06-30T23:59:00.000Z") });
    expect(isoDate).toStrictEqual("2026-07-02");
  });

  it("resolves environment with defaults when origin override is absent", () => {
    const resolved = resolveViewerCacheWarmEnvironment({
      pushEnv: {},
      viewerEnv: {
        PC_KEIBA_ACCESS_CLIENT_ID: "id-token",
        PC_KEIBA_ACCESS_CLIENT_SECRET: "secret-token",
      },
    });
    expect(resolved).toStrictEqual({
      origin: "https://pc-keiba-viewer.kkk4oru.com",
      skipWarm: false,
      credentials: {
        accessClientId: "id-token",
        accessClientSecret: "secret-token",
      },
    });
  });

  it("resolves environment with origin override", () => {
    const resolved = resolveViewerCacheWarmEnvironment({
      pushEnv: { REPLICA_PUSH_VIEWER_ORIGIN: "https://staging.example.com" },
      viewerEnv: {
        PC_KEIBA_ACCESS_CLIENT_ID: "id-token",
        PC_KEIBA_ACCESS_CLIENT_SECRET: "secret-token",
      },
    });
    expect(resolved).toStrictEqual({
      origin: "https://staging.example.com",
      skipWarm: false,
      credentials: {
        accessClientId: "id-token",
        accessClientSecret: "secret-token",
      },
    });
  });

  it("resolves environment with skip flag when set to 1", () => {
    const resolved = resolveViewerCacheWarmEnvironment({
      pushEnv: { REPLICA_PUSH_SKIP_VIEWER_WARM: "1" },
      viewerEnv: {
        PC_KEIBA_ACCESS_CLIENT_ID: "id-token",
        PC_KEIBA_ACCESS_CLIENT_SECRET: "secret-token",
      },
    });
    expect(resolved).toStrictEqual({
      origin: "https://pc-keiba-viewer.kkk4oru.com",
      skipWarm: true,
      credentials: {
        accessClientId: "id-token",
        accessClientSecret: "secret-token",
      },
    });
  });

  it("resolves environment with missing client id as null credentials", () => {
    const resolved = resolveViewerCacheWarmEnvironment({
      pushEnv: {},
      viewerEnv: { PC_KEIBA_ACCESS_CLIENT_SECRET: "secret-token" },
    });
    expect(resolved).toStrictEqual({
      origin: "https://pc-keiba-viewer.kkk4oru.com",
      skipWarm: false,
      credentials: null,
    });
  });

  it("resolves environment with missing client secret as null credentials", () => {
    const resolved = resolveViewerCacheWarmEnvironment({
      pushEnv: {},
      viewerEnv: { PC_KEIBA_ACCESS_CLIENT_ID: "id-token" },
    });
    expect(resolved).toStrictEqual({
      origin: "https://pc-keiba-viewer.kkk4oru.com",
      skipWarm: false,
      credentials: null,
    });
  });

  it("resolves environment with empty client id string as null credentials", () => {
    const resolved = resolveViewerCacheWarmEnvironment({
      pushEnv: {},
      viewerEnv: {
        PC_KEIBA_ACCESS_CLIENT_ID: "",
        PC_KEIBA_ACCESS_CLIENT_SECRET: "secret-token",
      },
    });
    expect(resolved).toStrictEqual({
      origin: "https://pc-keiba-viewer.kkk4oru.com",
      skipWarm: false,
      credentials: null,
    });
  });

  it("resolves environment with empty client secret string as null credentials", () => {
    const resolved = resolveViewerCacheWarmEnvironment({
      pushEnv: {},
      viewerEnv: {
        PC_KEIBA_ACCESS_CLIENT_ID: "id-token",
        PC_KEIBA_ACCESS_CLIENT_SECRET: "",
      },
    });
    expect(resolved).toStrictEqual({
      origin: "https://pc-keiba-viewer.kkk4oru.com",
      skipWarm: false,
      credentials: null,
    });
  });

  it("builds a request URL containing the iso date and required headers", () => {
    const request = buildCacheWarmRequest({
      origin: "https://example.com",
      endpoint: "race-detail-sections",
      isoDate: "2026-06-20",
      credentials: {
        accessClientId: "id-token",
        accessClientSecret: "secret-token",
      },
    });
    expect(request).toStrictEqual({
      url: "https://example.com/api/cache-warm/race-detail-sections?date=2026-06-20",
      init: {
        method: "POST",
        headers: {
          "X-PC-Keiba-Cache-Warm": "scheduled",
          "CF-Access-Client-Id": "id-token",
          "CF-Access-Client-Secret": "secret-token",
          "Content-Type": "application/json",
        },
      },
    });
  });

  it("fires a cache warm endpoint and summarizes enqueued + raceCount on success", async () => {
    const fetchImpl = vi.fn(async () =>
      createFakeResponse({
        ok: true,
        status: 200,
        jsonValue: { date: "2026-06-20", enqueued: 24, raceCount: 12 },
      }),
    );
    const result = await fireCacheWarmEndpoint({
      endpoint: "race-detail-sections",
      request: {
        url: "https://example.com/api/cache-warm/race-detail-sections?date=2026-06-20",
        init: { method: "POST" },
      },
      fetchImpl: fetchImpl as unknown as typeof fetch,
    });
    expect(result).toStrictEqual({
      endpoint: "race-detail-sections",
      outcome: "success",
      summary: "enqueued=24 raceCount=12",
    });
  });

  it("fires a cache warm endpoint and summarizes warmed + raceCount for ssr", async () => {
    const fetchImpl = vi.fn(async () =>
      createFakeResponse({
        ok: true,
        status: 200,
        jsonValue: { date: "2026-06-20", warmed: 11, raceCount: 12 },
      }),
    );
    const result = await fireCacheWarmEndpoint({
      endpoint: "race-detail-ssr",
      request: {
        url: "https://example.com/api/cache-warm/race-detail-ssr?date=2026-06-20",
        init: { method: "POST" },
      },
      fetchImpl: fetchImpl as unknown as typeof fetch,
    });
    expect(result).toStrictEqual({
      endpoint: "race-detail-ssr",
      outcome: "success",
      summary: "warmed=11 raceCount=12",
    });
  });

  it("fires a cache warm endpoint and summarizes dueRaceCount + enqueued for trends", async () => {
    const fetchImpl = vi.fn(async () =>
      createFakeResponse({
        ok: true,
        status: 200,
        jsonValue: {
          date: "2026-06-20",
          dueRaceCount: 7,
          enqueued: 14,
          raceCount: 12,
          variantsPerRace: 2,
        },
      }),
    );
    const result = await fireCacheWarmEndpoint({
      endpoint: "race-trends",
      request: {
        url: "https://example.com/api/cache-warm/race-trends?date=2026-06-20",
        init: { method: "POST" },
      },
      fetchImpl: fetchImpl as unknown as typeof fetch,
    });
    expect(result).toStrictEqual({
      endpoint: "race-trends",
      outcome: "success",
      summary: "enqueued=14 dueRaceCount=7 raceCount=12",
    });
  });

  it("returns failure outcome when response is not ok", async () => {
    const fetchImpl = vi.fn(async () =>
      createFakeResponse({ ok: false, status: 503, jsonValue: null }),
    );
    const result = await fireCacheWarmEndpoint({
      endpoint: "race-detail-sections",
      request: {
        url: "https://example.com/api/cache-warm/race-detail-sections?date=2026-06-20",
        init: { method: "POST" },
      },
      fetchImpl: fetchImpl as unknown as typeof fetch,
    });
    expect(result).toStrictEqual({
      endpoint: "race-detail-sections",
      outcome: "failure",
      summary: "HTTP 503",
    });
  });

  it("summarizes a non-object json body by stringifying it", async () => {
    const fetchImpl = vi.fn(async () =>
      createFakeResponse({ ok: true, status: 200, jsonValue: "ok" }),
    );
    const result = await fireCacheWarmEndpoint({
      endpoint: "race-detail-sections",
      request: {
        url: "https://example.com/api/cache-warm/race-detail-sections?date=2026-06-20",
        init: { method: "POST" },
      },
      fetchImpl: fetchImpl as unknown as typeof fetch,
    });
    expect(result).toStrictEqual({
      endpoint: "race-detail-sections",
      outcome: "success",
      summary: '"ok"',
    });
  });

  it("summarizes a success body without known fields by stringifying the record", async () => {
    const fetchImpl = vi.fn(async () =>
      createFakeResponse({ ok: true, status: 200, jsonValue: { date: "2026-06-20" } }),
    );
    const result = await fireCacheWarmEndpoint({
      endpoint: "race-detail-sections",
      request: {
        url: "https://example.com/api/cache-warm/race-detail-sections?date=2026-06-20",
        init: { method: "POST" },
      },
      fetchImpl: fetchImpl as unknown as typeof fetch,
    });
    expect(result).toStrictEqual({
      endpoint: "race-detail-sections",
      outcome: "success",
      summary: '{"date":"2026-06-20"}',
    });
  });

  it("logs success lines and does not throw when all three endpoints return ok", async () => {
    const logs: string[] = [];
    const fetchImpl = vi.fn(async (url: string) => {
      if (url.includes("race-detail-sections")) {
        return createFakeResponse({
          ok: true,
          status: 200,
          jsonValue: { enqueued: 24, raceCount: 12 },
        });
      }
      if (url.includes("race-detail-ssr")) {
        return createFakeResponse({
          ok: true,
          status: 200,
          jsonValue: { warmed: 11, raceCount: 12 },
        });
      }
      return createFakeResponse({
        ok: true,
        status: 200,
        jsonValue: { dueRaceCount: 7, enqueued: 14, raceCount: 12 },
      });
    });
    await warmViewerCachesForTomorrowJst({
      pushEnv: {},
      viewerEnv: {
        PC_KEIBA_ACCESS_CLIENT_ID: "id-token",
        PC_KEIBA_ACCESS_CLIENT_SECRET: "secret-token",
      },
      now: new Date("2026-06-19T12:00:00.000Z"),
      fetchImpl: fetchImpl as unknown as typeof fetch,
      log: (message) => logs.push(message),
    });
    expect(logs).toStrictEqual([
      "Warming viewer KV cache for tomorrow JST (2026-06-20)",
      "✓ race-detail-sections: enqueued=24 raceCount=12",
      "✓ race-detail-ssr: warmed=11 raceCount=12",
      "✓ race-trends: enqueued=14 dueRaceCount=7 raceCount=12",
    ]);
  });

  it("logs per-endpoint failure when one fetch rejects and lets others succeed", async () => {
    const logs: string[] = [];
    const fetchImpl = vi.fn(async (url: string) => {
      if (url.includes("race-detail-sections")) {
        throw new Error("ECONNREFUSED");
      }
      if (url.includes("race-detail-ssr")) {
        return createFakeResponse({
          ok: true,
          status: 200,
          jsonValue: { warmed: 11, raceCount: 12 },
        });
      }
      return createFakeResponse({
        ok: true,
        status: 200,
        jsonValue: { dueRaceCount: 7, enqueued: 14, raceCount: 12 },
      });
    });
    await warmViewerCachesForTomorrowJst({
      pushEnv: {},
      viewerEnv: {
        PC_KEIBA_ACCESS_CLIENT_ID: "id-token",
        PC_KEIBA_ACCESS_CLIENT_SECRET: "secret-token",
      },
      now: new Date("2026-06-19T12:00:00.000Z"),
      fetchImpl: fetchImpl as unknown as typeof fetch,
      log: (message) => logs.push(message),
    });
    expect(logs).toStrictEqual([
      "Warming viewer KV cache for tomorrow JST (2026-06-20)",
      "⚠ race-detail-sections failed: ECONNREFUSED",
      "✓ race-detail-ssr: warmed=11 raceCount=12",
      "✓ race-trends: enqueued=14 dueRaceCount=7 raceCount=12",
    ]);
  });

  it("logs per-endpoint failure when rejection reason is not an Error instance", async () => {
    const logs: string[] = [];
    const fetchImpl = vi.fn(async (url: string) => {
      if (url.includes("race-detail-sections")) {
        throw "stringified failure";
      }
      if (url.includes("race-detail-ssr")) {
        return createFakeResponse({
          ok: true,
          status: 200,
          jsonValue: { warmed: 11, raceCount: 12 },
        });
      }
      return createFakeResponse({
        ok: true,
        status: 200,
        jsonValue: { dueRaceCount: 7, enqueued: 14, raceCount: 12 },
      });
    });
    await warmViewerCachesForTomorrowJst({
      pushEnv: {},
      viewerEnv: {
        PC_KEIBA_ACCESS_CLIENT_ID: "id-token",
        PC_KEIBA_ACCESS_CLIENT_SECRET: "secret-token",
      },
      now: new Date("2026-06-19T12:00:00.000Z"),
      fetchImpl: fetchImpl as unknown as typeof fetch,
      log: (message) => logs.push(message),
    });
    expect(logs).toStrictEqual([
      "Warming viewer KV cache for tomorrow JST (2026-06-20)",
      "⚠ race-detail-sections failed: stringified failure",
      "✓ race-detail-ssr: warmed=11 raceCount=12",
      "✓ race-trends: enqueued=14 dueRaceCount=7 raceCount=12",
    ]);
  });

  it("logs HTTP failure summary when one endpoint returns non-ok response", async () => {
    const logs: string[] = [];
    const fetchImpl = vi.fn(async (url: string) => {
      if (url.includes("race-detail-sections")) {
        return createFakeResponse({ ok: false, status: 502, jsonValue: null });
      }
      if (url.includes("race-detail-ssr")) {
        return createFakeResponse({
          ok: true,
          status: 200,
          jsonValue: { warmed: 11, raceCount: 12 },
        });
      }
      return createFakeResponse({
        ok: true,
        status: 200,
        jsonValue: { dueRaceCount: 7, enqueued: 14, raceCount: 12 },
      });
    });
    await warmViewerCachesForTomorrowJst({
      pushEnv: {},
      viewerEnv: {
        PC_KEIBA_ACCESS_CLIENT_ID: "id-token",
        PC_KEIBA_ACCESS_CLIENT_SECRET: "secret-token",
      },
      now: new Date("2026-06-19T12:00:00.000Z"),
      fetchImpl: fetchImpl as unknown as typeof fetch,
      log: (message) => logs.push(message),
    });
    expect(logs).toStrictEqual([
      "Warming viewer KV cache for tomorrow JST (2026-06-20)",
      "⚠ race-detail-sections failed: HTTP 502",
      "✓ race-detail-ssr: warmed=11 raceCount=12",
      "✓ race-trends: enqueued=14 dueRaceCount=7 raceCount=12",
    ]);
  });

  it("skips warm and logs skip message when REPLICA_PUSH_SKIP_VIEWER_WARM=1", async () => {
    const logs: string[] = [];
    const fetchImpl = vi.fn(async () =>
      createFakeResponse({ ok: true, status: 200, jsonValue: {} }),
    );
    await warmViewerCachesForTomorrowJst({
      pushEnv: { REPLICA_PUSH_SKIP_VIEWER_WARM: "1" },
      viewerEnv: {
        PC_KEIBA_ACCESS_CLIENT_ID: "id-token",
        PC_KEIBA_ACCESS_CLIENT_SECRET: "secret-token",
      },
      now: new Date("2026-06-19T12:00:00.000Z"),
      fetchImpl: fetchImpl as unknown as typeof fetch,
      log: (message) => logs.push(message),
    });
    expect(logs).toStrictEqual(["Viewer cache warm skipped (REPLICA_PUSH_SKIP_VIEWER_WARM=1)"]);
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("skips warm and logs credentials missing message when env values absent", async () => {
    const logs: string[] = [];
    const fetchImpl = vi.fn(async () =>
      createFakeResponse({ ok: true, status: 200, jsonValue: {} }),
    );
    await warmViewerCachesForTomorrowJst({
      pushEnv: {},
      viewerEnv: {},
      now: new Date("2026-06-19T12:00:00.000Z"),
      fetchImpl: fetchImpl as unknown as typeof fetch,
      log: (message) => logs.push(message),
    });
    expect(logs).toStrictEqual(["⚠ viewer warm skipped: credentials missing"]);
    expect(fetchImpl).not.toHaveBeenCalled();
  });
});
