// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import {
  buildRunningStyleCacheRequest,
  getRunningStyleCacheTtlSeconds,
  putRunningStyleCache,
} from "./running-style-cache";
import type { RaceRunningStyleRow, RunningStyleInferenceRace } from "./running-style-d1";
import type { Env } from "./types";

interface CacheLike {
  put: (request: Request, response: Response) => Promise<void>;
}

interface CachesGlobal {
  default?: CacheLike;
}

const RACE: RunningStyleInferenceRace = {
  kaisaiNen: "2026",
  kaisaiTsukihi: "0512",
  keibajoCode: "08",
  raceBango: "01",
  raceKey: "jra:2026:0512:08:01",
  source: "jra",
};

const ROW: RaceRunningStyleRow = {
  bamei: "サンプル",
  category: "jra",
  horseNumber: 1,
  kaisaiNen: "2026",
  kettoTorokuBango: "2024100001",
  modelVersion: "v7-lineage",
  pNige: 0.1,
  pOikomi: 0.2,
  pSashi: 0.3,
  pSenkou: 0.4,
  predictedAt: "2026-05-12T11:30:00+09:00",
  predictedLabel: "senkou",
  raceKey: "jra:2026:0512:08:01",
};

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

it("getRunningStyleCacheTtlSeconds returns positive seconds before race-day end", () => {
  vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-05-12T10:00:00+09:00"));
  expect(
    getRunningStyleCacheTtlSeconds({ kaisaiNen: "2026", kaisaiTsukihi: "0512" }),
  ).toBe(50399);
});

it("getRunningStyleCacheTtlSeconds returns 0 after the race day", () => {
  vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-05-13T00:00:01+09:00"));
  expect(
    getRunningStyleCacheTtlSeconds({ kaisaiNen: "2026", kaisaiTsukihi: "0512" }),
  ).toBe(0);
});

it("getRunningStyleCacheTtlSeconds returns 0 for malformed kaisaiTsukihi", () => {
  expect(
    getRunningStyleCacheTtlSeconds({ kaisaiNen: "2026", kaisaiTsukihi: "zzzz" }),
  ).toBe(0);
});

it("buildRunningStyleCacheRequest uses the configured origin and appends search params", () => {
  const env = { RUNNING_STYLE_CACHE_ORIGIN: "https://configured.example" } as unknown as Env;
  const request = buildRunningStyleCacheRequest(env, RACE);
  expect(request.url).toBe(
    "https://configured.example/api/races/2026/05/12/08/01/running-styles?source=jra&__runningStyleCache=v3",
  );
});

it("buildRunningStyleCacheRequest falls back to default origin when env is blank", () => {
  const env = { RUNNING_STYLE_CACHE_ORIGIN: "  " } as unknown as Env;
  const request = buildRunningStyleCacheRequest(env, RACE);
  expect(request.url).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/races/2026/05/12/08/01/running-styles?source=jra&__runningStyleCache=v3",
  );
});

it("putRunningStyleCache returns false when rows is empty", async () => {
  const result = await putRunningStyleCache({
    env: {} as unknown as Env,
    race: RACE,
    rows: [],
  });
  expect(result).toBe(false);
});

it("putRunningStyleCache returns false when caches global is unavailable", async () => {
  const result = await putRunningStyleCache({
    env: {} as unknown as Env,
    race: RACE,
    rows: [ROW],
  });
  expect(result).toBe(false);
});

it("putRunningStyleCache returns false when ttl is zero (race day passed)", async () => {
  vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-05-13T00:00:01+09:00"));
  const put = vi.fn(async (_request: Request, _response: Response) => {});
  (globalThis as { caches?: CachesGlobal }).caches = { default: { put } };
  const result = await putRunningStyleCache({
    env: {} as unknown as Env,
    race: RACE,
    rows: [ROW],
  });
  expect(result).toBe(false);
  expect(put).not.toHaveBeenCalled();
});

it("putRunningStyleCache writes mapped rows when caches is available and ttl positive", async () => {
  vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-05-12T10:00:00+09:00"));
  const put = vi.fn(async (_request: Request, _response: Response) => {});
  (globalThis as { caches?: CachesGlobal }).caches = { default: { put } };
  const result = await putRunningStyleCache({
    env: {} as unknown as Env,
    race: RACE,
    rows: [ROW],
  });
  expect(result).toBe(true);
  expect(put).toHaveBeenCalledTimes(1);
  const response = put.mock.calls[0]![1];
  expect(response.headers.get("Content-Type")).toBe("application/json; charset=utf-8");
  expect(response.headers.get("Cache-Control")).toBe("public, max-age=50399");
  const body = await response.json();
  expect(body).toStrictEqual([
    {
      bamei: "サンプル",
      category: "jra",
      horseNumber: 1,
      kaisaiNen: "2026",
      kettoTorokuBango: "2024100001",
      modelVersion: "v7-lineage",
      p_nige: 0.1,
      p_oikomi: 0.2,
      p_sashi: 0.3,
      p_senkou: 0.4,
      predictedAt: "2026-05-12T11:30:00+09:00",
      predictedLabel: "senkou",
      raceKey: "jra:2026:0512:08:01",
    },
  ]);
});
