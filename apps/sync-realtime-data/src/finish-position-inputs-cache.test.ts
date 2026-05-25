// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import {
  buildFinishPositionInputsCacheRequest,
  getFinishPositionInputsCacheTtlSeconds,
  putFinishPositionInputsCache,
} from "./finish-position-inputs-cache";
import type { Env } from "./types";

interface CacheLike {
  put: (request: Request, response: Response) => Promise<void>;
}

interface CachesGlobal {
  default?: CacheLike;
}

const RACE = {
  kaisaiNen: "2026",
  kaisaiTsukihi: "0512",
  keibajoCode: "08",
  raceBango: "01",
  raceKey: "jra:2026:0512:08:01",
  source: "jra",
} as const;

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

it("returns a positive ttl when now precedes the race-day end", () => {
  vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-05-12T10:00:00+09:00"));
  const ttl = getFinishPositionInputsCacheTtlSeconds({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
  });
  expect(ttl).toBe(50399);
});

it("returns 0 when the race day has already passed", () => {
  vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-05-13T00:00:00+09:00"));
  const ttl = getFinishPositionInputsCacheTtlSeconds({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
  });
  expect(ttl).toBe(0);
});

it("returns 0 when kaisaiTsukihi is malformed", () => {
  const ttl = getFinishPositionInputsCacheTtlSeconds({
    kaisaiNen: "2026",
    kaisaiTsukihi: "zzzz",
  });
  expect(ttl).toBe(0);
});

it("uses the configured RUNNING_STYLE_CACHE_ORIGIN when set", () => {
  const env = { RUNNING_STYLE_CACHE_ORIGIN: "https://configured.example" } as unknown as Env;
  const request = buildFinishPositionInputsCacheRequest(env, RACE);
  expect(request.url).toBe(
    "https://configured.example/api/races/2026/05/12/08/01/sections/finish-prediction?source=jra&__finishPositionInputsCache=v1",
  );
});

it("falls back to the default origin when env is blank", () => {
  const env = { RUNNING_STYLE_CACHE_ORIGIN: "  " } as unknown as Env;
  const request = buildFinishPositionInputsCacheRequest(env, RACE);
  expect(request.url).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/races/2026/05/12/08/01/sections/finish-prediction?source=jra&__finishPositionInputsCache=v1",
  );
});

it("returns false when the caches global is not available", async () => {
  const env = {} as unknown as Env;
  const result = await putFinishPositionInputsCache({
    env,
    payload: {
      featuresR2Key: "k1",
      modelVersion: "v7-lineage",
      raceKey: RACE.raceKey,
    },
    race: RACE,
  });
  expect(result).toBe(false);
});

it("returns false when ttl is zero (race day already over)", async () => {
  vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-05-13T00:00:01+09:00"));
  const put = vi.fn(async (_request: Request, _response: Response) => {});
  (globalThis as { caches?: CachesGlobal }).caches = { default: { put } };
  const result = await putFinishPositionInputsCache({
    env: {} as unknown as Env,
    payload: {
      featuresR2Key: "k1",
      modelVersion: "v7-lineage",
      raceKey: RACE.raceKey,
    },
    race: RACE,
  });
  expect(result).toBe(false);
  expect(put).not.toHaveBeenCalled();
});

it("writes the cache when caches is available and ttl is positive", async () => {
  vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-05-12T10:00:00+09:00"));
  const put = vi.fn(async (_request: Request, _response: Response) => {});
  (globalThis as { caches?: CachesGlobal }).caches = { default: { put } };
  const env = {} as unknown as Env;
  const result = await putFinishPositionInputsCache({
    env,
    payload: {
      featuresR2Key: "key1",
      modelVersion: "v7-lineage",
      raceKey: RACE.raceKey,
    },
    race: RACE,
  });
  expect(result).toBe(true);
  expect(put).toHaveBeenCalledTimes(1);
  const response = put.mock.calls[0]![1];
  expect(response.headers.get("Content-Type")).toBe("application/json; charset=utf-8");
  expect(response.headers.get("Cache-Control")).toBe("public, max-age=50399");
  const body = await response.json();
  expect(body).toStrictEqual({
    featuresR2Key: "key1",
    modelVersion: "v7-lineage",
    raceKey: "jra:2026:0512:08:01",
  });
});
