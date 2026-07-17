// Run with bun. Tests for the focused-full R2 cache pickup (GET /focused-full-cache).

import { beforeEach, expect, test, vi } from "vitest";
import type { Env } from "./types";

const { proxyResultParquetsToRMock } = vi.hoisted(() => ({
  proxyResultParquetsToRMock: vi.fn(async () => undefined),
}));

vi.mock("./container-ndjson-proxy", () => ({
  proxyResultParquetsToR2: proxyResultParquetsToRMock,
}));

import { pickUpFocusedFullCache } from "./focused-full-cache-pickup";

const stubFetchMock = vi.fn();
const getMock = vi.fn(() => ({ fetch: stubFetchMock }));
const idFromNameMock = vi.fn(() => ({ name: "test-id" }));

const makeEnv = (): Env =>
  ({
    FEATURES_CACHE: {} as unknown as R2Bucket,
    FINISH_POSITION_PREDICT_CONTAINER: {
      get: getMock,
      idFromName: idFromNameMock,
    } as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
  }) as Env;

const jsonResponse = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), { status });

beforeEach(() => {
  stubFetchMock.mockClear();
  getMock.mockClear();
  idFromNameMock.mockClear();
  proxyResultParquetsToRMock.mockClear();
});

test("fetches the pickup endpoint with the expected URL and DO name", async () => {
  stubFetchMock.mockResolvedValue(jsonResponse({ found: false }));
  await pickUpFocusedFullCache({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "05",
    raceBango: "09",
    runYmd: "20260712",
  });
  expect(idFromNameMock).toHaveBeenCalledWith("predict-jra");
  const request = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(request.url).toBe(
    "http://do/focused-full-cache?category=jra&runDate=20260712&keibajoCode=05&raceBango=09",
  );
});

test("fetches the pickup endpoint at a race-sharded DO when RACE_SHARDED_DO is enabled", async () => {
  stubFetchMock.mockResolvedValue(jsonResponse({ found: false }));
  await pickUpFocusedFullCache({
    category: "jra",
    env: { ...makeEnv(), RACE_SHARDED_DO: "1" },
    keibajoCode: "05",
    raceBango: "09",
    runYmd: "20260712",
  });
  expect(idFromNameMock).toHaveBeenCalledWith("predict-jra-1");
});

test("proxies a found payload to R2 via proxyResultParquetsToR2", async () => {
  const env = makeEnv();
  stubFetchMock.mockResolvedValue(
    jsonResponse({
      found: true,
      parquetBase64: "YQ==",
      parquetKey: "feat-cache/nar/20260712/44/02/features.parquet",
      perRaceParquets: [{ parquetBase64: "cGE=", parquetKey: "per-race-key" }],
    }),
  );
  await pickUpFocusedFullCache({
    category: "nar",
    debug: true,
    env,
    keibajoCode: "44",
    raceBango: "02",
    runYmd: "20260712",
  });
  expect(proxyResultParquetsToRMock).toHaveBeenCalledTimes(1);
  expect(proxyResultParquetsToRMock).toHaveBeenCalledWith(
    {
      type: "result",
      category: "nar",
      racesPredicted: 0,
      parquetBase64: "YQ==",
      parquetKey: "feat-cache/nar/20260712/44/02/features.parquet",
      perRaceParquets: [{ parquetBase64: "cGE=", parquetKey: "per-race-key" }],
    },
    env,
    true,
  );
});

test("defaults debug to false when not provided", async () => {
  stubFetchMock.mockResolvedValue(
    jsonResponse({ found: true, parquetBase64: "YQ==", parquetKey: "k" }),
  );
  await pickUpFocusedFullCache({
    category: "ban-ei",
    env: makeEnv(),
    keibajoCode: "83",
    raceBango: "01",
    runYmd: "20260712",
  });
  expect(proxyResultParquetsToRMock).toHaveBeenCalledWith(
    expect.anything(),
    expect.anything(),
    false,
  );
});

test("normalizes null parquetBase64/parquetKey to undefined on the result line", async () => {
  const env = makeEnv();
  stubFetchMock.mockResolvedValue(
    jsonResponse({
      found: true,
      parquetBase64: null,
      parquetKey: null,
      perRaceParquets: [{ parquetBase64: "cGE=", parquetKey: "per-race-key" }],
    }),
  );
  await pickUpFocusedFullCache({
    category: "jra",
    env,
    keibajoCode: "05",
    raceBango: "09",
    runYmd: "20260712",
  });
  expect(proxyResultParquetsToRMock).toHaveBeenCalledWith(
    {
      type: "result",
      category: "jra",
      racesPredicted: 0,
      parquetBase64: undefined,
      parquetKey: undefined,
      perRaceParquets: [{ parquetBase64: "cGE=", parquetKey: "per-race-key" }],
    },
    env,
    false,
  );
});

test("does not proxy when found is false", async () => {
  stubFetchMock.mockResolvedValue(jsonResponse({ found: false }));
  await pickUpFocusedFullCache({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "05",
    raceBango: "09",
    runYmd: "20260712",
  });
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
});

test("warns and does not proxy on a non-ok response status", async () => {
  const consoleWarn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  stubFetchMock.mockResolvedValue(jsonResponse({ found: true }, 500));
  await pickUpFocusedFullCache({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "05",
    raceBango: "09",
    runYmd: "20260712",
  });
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
  expect(consoleWarn).toHaveBeenCalledWith(expect.stringContaining("non-ok status=500"));
  consoleWarn.mockRestore();
});

test("swallows and warns when the DO fetch rejects", async () => {
  const consoleWarn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  stubFetchMock.mockRejectedValue(new Error("container unreachable"));
  await expect(
    pickUpFocusedFullCache({
      category: "jra",
      env: makeEnv(),
      keibajoCode: "05",
      raceBango: "09",
      runYmd: "20260712",
    }),
  ).resolves.toBeUndefined();
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
  expect(consoleWarn).toHaveBeenCalledWith(
    expect.stringContaining(
      "failed category=jra runYmd=20260712 keibajo=05 race=09: Error: container unreachable",
    ),
  );
  consoleWarn.mockRestore();
});

test("swallows and warns when the response body is malformed JSON", async () => {
  const consoleWarn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  stubFetchMock.mockResolvedValue(new Response("not json", { status: 200 }));
  await expect(
    pickUpFocusedFullCache({
      category: "jra",
      env: makeEnv(),
      keibajoCode: "05",
      raceBango: "09",
      runYmd: "20260712",
    }),
  ).resolves.toBeUndefined();
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
  expect(consoleWarn).toHaveBeenCalledTimes(1);
  consoleWarn.mockRestore();
});

test("swallows and warns when proxyResultParquetsToR2 itself rejects", async () => {
  const consoleWarn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  stubFetchMock.mockResolvedValue(
    jsonResponse({ found: true, parquetBase64: "YQ==", parquetKey: "k" }),
  );
  proxyResultParquetsToRMock.mockRejectedValueOnce(new Error("r2 put failed"));
  await expect(
    pickUpFocusedFullCache({
      category: "jra",
      env: makeEnv(),
      keibajoCode: "05",
      raceBango: "09",
      runYmd: "20260712",
    }),
  ).resolves.toBeUndefined();
  expect(consoleWarn).toHaveBeenCalledWith(expect.stringContaining("r2 put failed"));
  consoleWarn.mockRestore();
});
