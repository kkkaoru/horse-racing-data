// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import {
  buildPremiumDataTopCacheRequest,
  getPremiumDataTopCacheTtlSeconds,
  putPremiumDataTopCache,
} from "./premium-data-top-cache";

interface CacheLike {
  put: (request: Request, response: Response) => Promise<void>;
}

interface CachesGlobal {
  default?: CacheLike;
}

const originalCaches = (globalThis as { caches?: CachesGlobal }).caches;

beforeEach(() => {
  delete (globalThis as { caches?: CachesGlobal }).caches;
});

afterEach(() => {
  vi.restoreAllMocks();
  if (originalCaches === undefined) {
    delete (globalThis as { caches?: CachesGlobal }).caches;
  } else {
    (globalThis as { caches?: CachesGlobal }).caches = originalCaches;
  }
});

it("returns positive ttl when now is before race day end", () => {
  vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-05-12T10:00:00+09:00"));
  const ttl = getPremiumDataTopCacheTtlSeconds({ kaisaiNen: "2026", kaisaiTsukihi: "0512" });
  expect(ttl).toBe(50399);
});

it("returns zero when race day has already ended", () => {
  vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-05-13T10:00:00+09:00"));
  const ttl = getPremiumDataTopCacheTtlSeconds({ kaisaiNen: "2026", kaisaiTsukihi: "0512" });
  expect(ttl).toBe(0);
});

it("returns zero when kaisaiTsukihi is malformed", () => {
  const ttl = getPremiumDataTopCacheTtlSeconds({ kaisaiNen: "2026", kaisaiTsukihi: "zzzz" });
  expect(ttl).toBe(0);
});

it("uses default origin when env is empty", () => {
  const request = buildPremiumDataTopCacheRequest(
    {},
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      raceBango: "01",
      source: "jra",
    },
  );
  expect(request.url).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/races/2026/05/12/08/01/sections/premium-data-top?source=jra&__premiumDataTopCache=v1",
  );
});

it("prefers PREMIUM_DATA_TOP_CACHE_ORIGIN over RUNNING_STYLE_CACHE_ORIGIN", () => {
  const request = buildPremiumDataTopCacheRequest(
    {
      PREMIUM_DATA_TOP_CACHE_ORIGIN: "https://primary.example",
      RUNNING_STYLE_CACHE_ORIGIN: "https://fallback.example",
    },
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      raceBango: "01",
      source: "jra",
    },
  );
  expect(request.url).toBe(
    "https://primary.example/api/races/2026/05/12/08/01/sections/premium-data-top?source=jra&__premiumDataTopCache=v1",
  );
});

it("falls back to RUNNING_STYLE_CACHE_ORIGIN when PREMIUM_DATA_TOP_CACHE_ORIGIN is blank", () => {
  const request = buildPremiumDataTopCacheRequest(
    {
      PREMIUM_DATA_TOP_CACHE_ORIGIN: "  ",
      RUNNING_STYLE_CACHE_ORIGIN: "https://fallback.example",
    },
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      raceBango: "01",
      source: "jra",
    },
  );
  expect(request.url).toBe(
    "https://fallback.example/api/races/2026/05/12/08/01/sections/premium-data-top?source=jra&__premiumDataTopCache=v1",
  );
});

it("returns false when rows is empty", async () => {
  const result = await putPremiumDataTopCache({
    env: {},
    race: {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      raceBango: "01",
      source: "jra",
    },
    rows: [],
  });
  expect(result).toBe(false);
});

it("returns false when caches global is unavailable", async () => {
  const result = await putPremiumDataTopCache({
    env: {},
    race: {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      raceBango: "01",
      source: "jra",
    },
    rows: [
      {
        fetchedAt: "2026-05-12T11:00:00+09:00",
        horseName: "sample",
        horseNumber: "1",
        rank: 1,
        reasons: [],
      },
    ],
  });
  expect(result).toBe(false);
});

it("returns false when ttl is zero (race day already over)", async () => {
  vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-05-13T00:00:01+09:00"));
  const put = vi.fn(async (_request: Request, _response: Response) => {});
  (globalThis as { caches?: CachesGlobal }).caches = { default: { put } };
  const result = await putPremiumDataTopCache({
    env: {},
    race: {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      raceBango: "01",
      source: "jra",
    },
    rows: [
      {
        fetchedAt: "2026-05-12T11:00:00+09:00",
        horseName: "sample",
        horseNumber: "1",
        rank: 1,
        reasons: [],
      },
    ],
  });
  expect(result).toBe(false);
  expect(put).not.toHaveBeenCalled();
});

it("writes the cache entry when caches is available and ttl is positive", async () => {
  vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-05-12T10:00:00+09:00"));
  const put = vi.fn(async (_request: Request, _response: Response) => {});
  (globalThis as { caches?: CachesGlobal }).caches = { default: { put } };
  const result = await putPremiumDataTopCache({
    env: { PREMIUM_DATA_TOP_CACHE_ORIGIN: "https://primary.example" },
    race: {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      raceBango: "01",
      source: "jra",
    },
    rows: [
      {
        fetchedAt: "2026-05-12T11:00:00+09:00",
        horseName: "sample",
        horseNumber: "1",
        rank: 1,
        reasons: [],
      },
    ],
  });
  expect(result).toBe(true);
  expect(put).toHaveBeenCalledTimes(1);
  const request = put.mock.calls[0]![0];
  const response = put.mock.calls[0]![1];
  expect(request.url).toBe(
    "https://primary.example/api/races/2026/05/12/08/01/sections/premium-data-top?source=jra&__premiumDataTopCache=v1",
  );
  expect(response.headers.get("Content-Type")).toBe("application/json; charset=utf-8");
  expect(response.headers.get("Cache-Control")).toBe("public, max-age=50399");
  const body = await response.json();
  expect(body).toStrictEqual({
    dataTopHorses: [
      {
        fetchedAt: "2026-05-12T11:00:00+09:00",
        horseName: "sample",
        horseNumber: "1",
        rank: 1,
        reasons: [],
      },
    ],
    type: "premium-data-top",
  });
});
