// Run with bun. Tests for day-base R2 pickup via FEATURES_CACHE.

import { beforeEach, expect, test, vi } from "vitest";
import type { Env } from "./types";

const {
  getDayBaseCandidateReadinessMock,
  materializeDayBasePerRaceCacheMock,
  proxyResultParquetsToRMock,
} = vi.hoisted(() => ({
  getDayBaseCandidateReadinessMock: vi.fn(async () => ({ ready: true, reason: "ready" })),
  materializeDayBasePerRaceCacheMock: vi.fn(
    async (): Promise<{ status: "materialized" } | { reason: string; status: "fallback" }> => ({
      status: "materialized",
    }),
  ),
  proxyResultParquetsToRMock: vi.fn(async () => undefined),
}));

vi.mock("./container-ndjson-proxy", () => ({
  proxyResultParquetsToR2: proxyResultParquetsToRMock,
}));

vi.mock("./day-base-race-materializer", () => ({
  materializeDayBasePerRaceCache: materializeDayBasePerRaceCacheMock,
}));

vi.mock("./focused-full-day-base-readiness", () => ({
  getDayBaseCandidateReadiness: getDayBaseCandidateReadinessMock,
}));

import {
  buildDayBaseObjectKey,
  headDayBaseObject,
  pickUpPrewarmDayBase,
  pickUpPrewarmDayBaseWithOutcome,
} from "./day-base-prewarm-pickup";

interface PickupFeaturesCache {
  head: ReturnType<typeof vi.fn>;
}

const stubFetchMock = vi.fn();
const getMock = vi.fn(() => ({ fetch: stubFetchMock }));
const idFromNameMock = vi.fn(() => ({ name: "test-id" }));
const headMock = vi.fn();

const createPickupEnv = (cache: PickupFeaturesCache): Env =>
  ({
    FEATURES_CACHE: cache,
    FINISH_POSITION_PREDICT_CONTAINER: {
      get: getMock,
      idFromName: idFromNameMock,
    },
  }) as unknown as Env;

const isRequest = (value: unknown): value is Request =>
  typeof value === "object" && value !== null && "url" in value;

beforeEach(() => {
  stubFetchMock.mockClear();
  getMock.mockClear();
  idFromNameMock.mockClear();
  headMock.mockClear();
  materializeDayBasePerRaceCacheMock.mockClear();
  materializeDayBasePerRaceCacheMock.mockResolvedValue({ status: "materialized" });
  proxyResultParquetsToRMock.mockClear();
  getDayBaseCandidateReadinessMock.mockReset();
  getDayBaseCandidateReadinessMock.mockResolvedValue({ ready: true, reason: "ready" });
});

test("buildDayBaseObjectKey is the catalog-v1 day-base path", () => {
  expect(buildDayBaseObjectKey({ category: "ban-ei", runYmd: "20260816" })).toBe(
    "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
  );
});

test("headDayBaseObject ignores an object that has no watermark metadata", async () => {
  headMock.mockResolvedValueOnce({ size: 10 });
  const found = await headDayBaseObject({
    category: "ban-ei",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(found).toBe(null);
  expect(headMock).toHaveBeenCalledWith("feat-daybase/catalog-v1/ban-ei/20260816/features.parquet");
});

test("headDayBaseObject ignores an object whose rs-predicted-at-max is empty", async () => {
  headMock.mockResolvedValueOnce({
    customMetadata: {
      "max-data-sakusei-nengappi": "20260816",
      "row-count": "80",
      "rs-predicted-at-max": "",
      "rs-row-count": "0",
    },
    size: 87257,
  });
  const found = await headDayBaseObject({
    category: "ban-ei",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(found).toBe(null);
});

test("headDayBaseObject ignores an object whose watermark metadata is empty", async () => {
  headMock.mockResolvedValueOnce({
    customMetadata: {
      "max-data-sakusei-nengappi": "",
      "row-count": "80",
      "rs-predicted-at-max": "2026-08-16T00:00:00",
      "rs-row-count": "4",
    },
    size: 87257,
  });
  const found = await headDayBaseObject({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(found).toBe(null);
});

test("headDayBaseObject accepts Ban-ei none RS token", async () => {
  headMock.mockResolvedValueOnce({
    customMetadata: {
      "max-data-sakusei-nengappi": "20260816",
      "row-count": "80",
      "rs-predicted-at-max": "none",
      "rs-row-count": "0",
    },
    size: 87257,
  });
  const found = await headDayBaseObject({
    category: "ban-ei",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(found).toStrictEqual({
    customMetadata: {
      "max-data-sakusei-nengappi": "20260816",
      "row-count": "80",
      "rs-predicted-at-max": "none",
      "rs-row-count": "0",
    },
    size: 87257,
  });
});

test("headDayBaseObject ignores an object whose row-count metadata is not numeric", async () => {
  headMock.mockResolvedValueOnce({
    customMetadata: {
      "max-data-sakusei-nengappi": "20260816",
      "row-count": "",
      "rs-predicted-at-max": "2026-08-16T00:00:00",
      "rs-row-count": "4",
    },
    size: 87257,
  });
  const found = await headDayBaseObject({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(found).toBe(null);
});

test("headDayBaseObject ignores an object whose rs-row-count metadata is not numeric", async () => {
  headMock.mockResolvedValueOnce({
    customMetadata: {
      "max-data-sakusei-nengappi": "20260816",
      "row-count": "80",
      "rs-predicted-at-max": "2026-08-16T00:00:00",
      "rs-row-count": "",
    },
    size: 87257,
  });
  const found = await headDayBaseObject({
    category: "nar",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(found).toBe(null);
});

test("headDayBaseObject ignores an object missing max-updated metadata", async () => {
  headMock.mockResolvedValueOnce({
    customMetadata: {
      "row-count": "80",
      "rs-predicted-at-max": "2026-08-16T00:00:00",
      "rs-row-count": "4",
    },
    size: 87257,
  });
  const found = await headDayBaseObject({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(found).toBe(null);
});

test("headDayBaseObject ignores an object missing row-count metadata", async () => {
  headMock.mockResolvedValueOnce({
    customMetadata: {
      "max-data-sakusei-nengappi": "20260816",
      "rs-predicted-at-max": "2026-08-16T00:00:00",
      "rs-row-count": "4",
    },
    size: 87257,
  });
  const found = await headDayBaseObject({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(found).toBe(null);
});

test("headDayBaseObject ignores an object missing rs-predicted-at-max metadata", async () => {
  headMock.mockResolvedValueOnce({
    customMetadata: {
      "max-data-sakusei-nengappi": "20260816",
      "row-count": "80",
      "rs-row-count": "4",
    },
    size: 87257,
  });
  const found = await headDayBaseObject({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(found).toBe(null);
});

test("headDayBaseObject ignores an object missing rs-row-count metadata", async () => {
  headMock.mockResolvedValueOnce({
    customMetadata: {
      "max-data-sakusei-nengappi": "20260816",
      "row-count": "80",
      "rs-predicted-at-max": "2026-08-16T00:00:00",
    },
    size: 87257,
  });
  const found = await headDayBaseObject({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(found).toBe(null);
});

test("headDayBaseObject returns a watermarked day-base object", async () => {
  headMock.mockResolvedValueOnce({
    customMetadata: {
      "max-data-sakusei-nengappi": "20260816",
      "row-count": "80",
      "rs-predicted-at-max": "2026-08-16T00:00:00",
      "rs-row-count": "4",
    },
    size: 87257,
  });
  const found = await headDayBaseObject({
    category: "ban-ei",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(found).toStrictEqual({
    customMetadata: {
      "max-data-sakusei-nengappi": "20260816",
      "row-count": "80",
      "rs-predicted-at-max": "2026-08-16T00:00:00",
      "rs-row-count": "4",
    },
    size: 87257,
  });
});

test("pickUpPrewarmDayBase fetches the category-scoped cache endpoint", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(JSON.stringify({ found: false }), { status: 200 }),
  );
  await pickUpPrewarmDayBase({
    category: "ban-ei",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(idFromNameMock).toHaveBeenCalledWith("predict-ban-ei");
  const firstCall = stubFetchMock.mock.calls[0];
  if (firstCall === undefined) throw new Error("expected a fetch");
  const request = firstCall[0];
  if (!isRequest(request)) throw new Error("expected a Request");
  expect(request.url).toBe("http://do/prewarm-day-base-cache?category=ban-ei&runDate=20260816");
});

test("pickUpPrewarmDayBase PUTs a found payload through FEATURES_CACHE proxy", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        found: true,
        parquetBase64: "YQ==",
        parquetKey: "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
        daybaseWatermark: {
          maxDataSakuseiNengappi: "20260816",
          rowCount: 80,
          rsPredictedAtMax: "2026-08-16T00:00:00",
          rsRowCount: 4,
        },
      }),
      { status: 200 },
    ),
  );
  const env: Env = createPickupEnv({ head: headMock });
  const ok = await pickUpPrewarmDayBase({
    category: "ban-ei",
    debug: true,
    env,
    runYmd: "20260816",
  });
  expect(ok).toBe(true);
  expect(proxyResultParquetsToRMock).toHaveBeenCalledTimes(1);
  expect(proxyResultParquetsToRMock).toHaveBeenCalledWith(
    {
      type: "result",
      category: "ban-ei",
      racesPredicted: 0,
      parquetBase64: "YQ==",
      parquetKey: "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
      daybaseWatermark: {
        maxDataSakuseiNengappi: "20260816",
        rowCount: 80,
        rsPredictedAtMax: "2026-08-16T00:00:00",
        rsRowCount: 4,
      },
    },
    env,
    true,
  );
  expect(materializeDayBasePerRaceCacheMock).toHaveBeenCalledWith({
    category: "ban-ei",
    env,
    runYmd: "20260816",
  });
});

test("pickUpPrewarmDayBase fails closed when the per-race Worker cache cannot materialize", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        found: true,
        parquetBase64: "YQ==",
        parquetKey: "feat-daybase/catalog-v1/nar/20260826/features.parquet",
        daybaseWatermark: {
          maxDataSakuseiNengappi: "20260826",
          rowCount: 446,
          rsPredictedAtMax: "2026-08-25T12:10:07.376Z",
          rsRowCount: 446,
        },
      }),
      { status: 200 },
    ),
  );
  materializeDayBasePerRaceCacheMock.mockResolvedValueOnce({
    reason: "manifest-write-failed",
    status: "fallback",
  });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await expect(
    pickUpPrewarmDayBaseWithOutcome({
      category: "nar",
      env: createPickupEnv({ head: headMock }),
      runYmd: "20260826",
    }),
  ).resolves.toBe("transient-error");
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm-pickup] per-race foundation warm failed category=nar runYmd=20260826 reason=manifest-write-failed",
  );
  warnSpy.mockRestore();
});

test("pickUpPrewarmDayBaseWithOutcome lands the exact running-style foundation without final readiness", async () => {
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      found: true,
      parquetBase64: "YQ==",
      parquetKey: "feat-running-style-base/catalog-v1/nar/20260824/features.parquet",
      daybaseWatermark: {
        maxDataSakuseiNengappi: "20260824190000",
        rowCount: 479,
        rsPredictedAtMax: "none",
        rsRowCount: 0,
      },
    }),
  );
  const env: Env = createPickupEnv({ head: headMock });

  const outcome = await pickUpPrewarmDayBaseWithOutcome({
    category: "nar",
    env,
    runYmd: "20260824",
  });

  expect(outcome).toBe("foundation-landed");
  expect(proxyResultParquetsToRMock).toHaveBeenCalledWith(
    {
      type: "result",
      category: "nar",
      racesPredicted: 0,
      parquetBase64: "YQ==",
      parquetKey: "feat-running-style-base/catalog-v1/nar/20260824/features.parquet",
      daybaseWatermark: {
        maxDataSakuseiNengappi: "20260824190000",
        rowCount: 479,
        rsPredictedAtMax: "none",
        rsRowCount: 0,
      },
    },
    env,
    false,
  );
  expect(getDayBaseCandidateReadinessMock).not.toHaveBeenCalled();
  expect(materializeDayBasePerRaceCacheMock).not.toHaveBeenCalled();
});

test("pickUpPrewarmDayBaseWithOutcome rejects a payload outside both exact cache slots", async () => {
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      found: true,
      parquetBase64: "YQ==",
      parquetKey: "feat-running-style-base/catalog-v1/jra/20260824/features.parquet",
      daybaseWatermark: {
        maxDataSakuseiNengappi: "20260824190000",
        rowCount: 479,
        rsPredictedAtMax: "none",
        rsRowCount: 0,
      },
    }),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  const outcome = await pickUpPrewarmDayBaseWithOutcome({
    category: "nar",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260824",
  });

  expect(outcome).toBe("rejected");
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
  expect(getDayBaseCandidateReadinessMock).not.toHaveBeenCalled();
  expect(materializeDayBasePerRaceCacheMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm-pickup] rejected unexpected key category=nar runYmd=20260824 key=feat-running-style-base/catalog-v1/jra/20260824/features.parquet",
  );
  warnSpy.mockRestore();
});

test("pickUpPrewarmDayBase rejects a stale candidate before canonical R2 PUT", async () => {
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      found: true,
      parquetBase64: "YQ==",
      parquetKey: "feat-daybase/catalog-v1/jra/20260823/features.parquet",
      daybaseWatermark: {
        maxDataSakuseiNengappi: "20260823090000",
        rowCount: 26,
        rsPredictedAtMax: "2026-08-23T00:05:00Z",
        rsRowCount: 26,
      },
    }),
  );
  getDayBaseCandidateReadinessMock.mockResolvedValueOnce({
    ready: false,
    reason: "source-row-count-26-of-392",
  });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  const ok = await pickUpPrewarmDayBase({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260823",
  });

  expect(ok).toBe(false);
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
  expect(materializeDayBasePerRaceCacheMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm-pickup] rejected stale candidate category=jra runYmd=20260823 reason=source-row-count-26-of-392",
  );
  warnSpy.mockRestore();
});

test("pickUpPrewarmDayBaseWithOutcome distinguishes a stale candidate from a missing one", async () => {
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      found: true,
      parquetBase64: "YQ==",
      parquetKey: "feat-daybase/catalog-v1/nar/20260824/features.parquet",
      daybaseWatermark: {
        maxDataSakuseiNengappi: "20260824030000",
        rowCount: 479,
        rsPredictedAtMax: "2026-08-24T03:00:00Z",
        rsRowCount: 368,
      },
    }),
  );
  getDayBaseCandidateReadinessMock.mockResolvedValueOnce({
    ready: false,
    reason: "rs-row-count-368-of-479",
  });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  const outcome = await pickUpPrewarmDayBaseWithOutcome({
    category: "nar",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260824",
  });

  expect(outcome).toBe("stale");
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("pickUpPrewarmDayBase cannot report success when canonical R2 PUT rejects", async () => {
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      found: true,
      parquetBase64: "YQ==",
      parquetKey: "feat-daybase/catalog-v1/jra/20260823/features.parquet",
      daybaseWatermark: {
        maxDataSakuseiNengappi: "20260823090000",
        rowCount: 392,
        rsPredictedAtMax: "2026-08-23T00:05:00Z",
        rsRowCount: 392,
      },
    }),
  );
  proxyResultParquetsToRMock.mockRejectedValueOnce(new Error("canonical put failed"));
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  const ok = await pickUpPrewarmDayBase({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260823",
  });

  expect(ok).toBe(false);
  expect(materializeDayBasePerRaceCacheMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("canonical put failed"));
  warnSpy.mockRestore();
});

test("pickUpPrewarmDayBase PUTs Ban-ei watermark with none RS token", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        found: true,
        parquetBase64: "YQ==",
        parquetKey: "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
        daybaseWatermark: {
          maxDataSakuseiNengappi: "20260816",
          rowCount: 80,
          rsPredictedAtMax: "none",
          rsRowCount: 0,
        },
      }),
      { status: 200 },
    ),
  );
  const env: Env = createPickupEnv({ head: headMock });
  const ok = await pickUpPrewarmDayBase({
    category: "ban-ei",
    debug: true,
    env,
    runYmd: "20260816",
  });
  expect(ok).toBe(true);
  expect(proxyResultParquetsToRMock).toHaveBeenCalledTimes(1);
  expect(proxyResultParquetsToRMock).toHaveBeenCalledWith(
    {
      type: "result",
      category: "ban-ei",
      racesPredicted: 0,
      parquetBase64: "YQ==",
      parquetKey: "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
      daybaseWatermark: {
        maxDataSakuseiNengappi: "20260816",
        rowCount: 80,
        rsPredictedAtMax: "none",
        rsRowCount: 0,
      },
    },
    env,
    true,
  );
});

test("pickUpPrewarmDayBase returns false when maxDataSakuseiNengappi is empty", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        found: true,
        parquetBase64: "YQ==",
        parquetKey: "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
        daybaseWatermark: {
          maxDataSakuseiNengappi: "",
          rowCount: 80,
          rsPredictedAtMax: "none",
          rsRowCount: 0,
        },
      }),
      { status: 200 },
    ),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const ok = await pickUpPrewarmDayBase({
    category: "ban-ei",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm-pickup] missing watermark category=ban-ei runYmd=20260816 reason=-",
  );
  warnSpy.mockRestore();
});

test("pickUpPrewarmDayBase returns false when rsPredictedAtMax is empty", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        found: true,
        parquetBase64: "YQ==",
        parquetKey: "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
        daybaseWatermark: {
          maxDataSakuseiNengappi: "20260816",
          rowCount: 80,
          rsPredictedAtMax: "",
          rsRowCount: 0,
        },
      }),
      { status: 200 },
    ),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const ok = await pickUpPrewarmDayBase({
    category: "ban-ei",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm-pickup] missing watermark category=ban-ei runYmd=20260816 reason=-",
  );
  warnSpy.mockRestore();
});

test("pickUpPrewarmDayBase defaults debug to false when not provided", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        found: true,
        parquetBase64: "YQ==",
        parquetKey: "feat-daybase/catalog-v1/jra/20260816/features.parquet",
        daybaseWatermark: {
          maxDataSakuseiNengappi: "20260816",
          rowCount: 12,
          rsPredictedAtMax: "2026-08-16T00:00:00",
          rsRowCount: 2,
        },
      }),
      { status: 200 },
    ),
  );
  const env: Env = createPickupEnv({ head: headMock });
  const ok = await pickUpPrewarmDayBase({
    category: "jra",
    env,
    runYmd: "20260816",
  });
  expect(ok).toBe(true);
  expect(proxyResultParquetsToRMock).toHaveBeenCalledWith(
    {
      type: "result",
      category: "jra",
      racesPredicted: 0,
      parquetBase64: "YQ==",
      parquetKey: "feat-daybase/catalog-v1/jra/20260816/features.parquet",
      daybaseWatermark: {
        maxDataSakuseiNengappi: "20260816",
        rowCount: 12,
        rsPredictedAtMax: "2026-08-16T00:00:00",
        rsRowCount: 2,
      },
    },
    env,
    false,
  );
});

test("pickUpPrewarmDayBase returns false when the payload is missing", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(JSON.stringify({ found: false }), { status: 200 }),
  );
  const ok = await pickUpPrewarmDayBase({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
});

test("pickUpPrewarmDayBase returns false when the JSON body is not a record", async () => {
  stubFetchMock.mockResolvedValueOnce(new Response("null", { status: 200 }));
  const ok = await pickUpPrewarmDayBase({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
});

test("pickUpPrewarmDayBase returns false when found is not a boolean", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(JSON.stringify({ found: "yes" }), { status: 200 }),
  );
  const ok = await pickUpPrewarmDayBase({
    category: "nar",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
});

test("pickUpPrewarmDayBase returns false when parquetBase64 is missing", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        found: true,
        parquetKey: "feat-daybase/catalog-v1/jra/20260816/features.parquet",
      }),
      { status: 200 },
    ),
  );
  const ok = await pickUpPrewarmDayBase({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
});

test("pickUpPrewarmDayBase returns false when parquetBase64 is null", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        found: true,
        parquetBase64: null,
        parquetKey: "feat-daybase/catalog-v1/jra/20260816/features.parquet",
      }),
      { status: 200 },
    ),
  );
  const ok = await pickUpPrewarmDayBase({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
});

test("pickUpPrewarmDayBase returns false when parquetBase64 is not a string", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        found: true,
        parquetBase64: 1,
        parquetKey: "feat-daybase/catalog-v1/jra/20260816/features.parquet",
      }),
      { status: 200 },
    ),
  );
  const ok = await pickUpPrewarmDayBase({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
});

test("pickUpPrewarmDayBase returns false when parquetKey is missing", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(JSON.stringify({ found: true, parquetBase64: "YQ==" }), { status: 200 }),
  );
  const ok = await pickUpPrewarmDayBase({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
});

test("pickUpPrewarmDayBase returns false when parquetKey is null", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(JSON.stringify({ found: true, parquetBase64: "YQ==", parquetKey: null }), {
      status: 200,
    }),
  );
  const ok = await pickUpPrewarmDayBase({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
});

test("pickUpPrewarmDayBase returns false when parquetKey is not a string", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(JSON.stringify({ found: true, parquetBase64: "YQ==", parquetKey: 1 }), {
      status: 200,
    }),
  );
  const ok = await pickUpPrewarmDayBase({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
});

test("pickUpPrewarmDayBase returns false when parquetBase64 is empty", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        found: true,
        parquetBase64: "",
        parquetKey: "feat-daybase/catalog-v1/jra/20260816/features.parquet",
      }),
      { status: 200 },
    ),
  );
  const ok = await pickUpPrewarmDayBase({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
});

test("pickUpPrewarmDayBase returns false when parquetKey is empty", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(JSON.stringify({ found: true, parquetBase64: "YQ==", parquetKey: "" }), {
      status: 200,
    }),
  );
  const ok = await pickUpPrewarmDayBase({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
});

test("pickUpPrewarmDayBase returns false on a non-ok container response", async () => {
  stubFetchMock.mockResolvedValueOnce(new Response("nope", { status: 500 }));
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const ok = await pickUpPrewarmDayBase({
    category: "nar",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm-pickup] non-ok status=500 doName=predict-nar url=http://do/prewarm-day-base-cache?category=nar&runDate=20260816",
  );
  warnSpy.mockRestore();
});

test("pickUpPrewarmDayBase returns false when fetch rejects", async () => {
  stubFetchMock.mockRejectedValueOnce(new Error("network down"));
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const ok = await pickUpPrewarmDayBase({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm-pickup] failed category=jra runYmd=20260816 doName=predict-jra: Error: network down",
  );
  warnSpy.mockRestore();
});

test("pickUpPrewarmDayBaseWithOutcome keeps a fetch failure distinct from a cache miss", async () => {
  stubFetchMock.mockRejectedValueOnce(new Error("network down"));
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  const outcome = await pickUpPrewarmDayBaseWithOutcome({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });

  expect(outcome).toBe("transient-error");
  warnSpy.mockRestore();
});

test("pickUpPrewarmDayBase returns false when the daybase watermark is missing", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        found: true,
        parquetBase64: "YQ==",
        parquetKey: "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
      }),
      { status: 200 },
    ),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const ok = await pickUpPrewarmDayBase({
    category: "ban-ei",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm-pickup] missing watermark category=ban-ei runYmd=20260816 reason=-",
  );
  warnSpy.mockRestore();
});

test("pickUpPrewarmDayBase logs watermarkError and does not PUT to R2", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        found: true,
        parquetBase64: "YQ==",
        parquetKey: "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
        watermarkError: "watermark count is 0",
      }),
      { status: 200 },
    ),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const ok = await pickUpPrewarmDayBase({
    category: "ban-ei",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm-pickup] missing watermark category=ban-ei runYmd=20260816 reason=watermark count is 0",
  );
  warnSpy.mockRestore();
});

test("pickUpPrewarmDayBase returns false when maxDataSakuseiNengappi is not a string", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        found: true,
        parquetBase64: "YQ==",
        parquetKey: "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
        daybaseWatermark: {
          maxDataSakuseiNengappi: 20260816,
          rowCount: 80,
          rsPredictedAtMax: "",
          rsRowCount: 0,
        },
      }),
      { status: 200 },
    ),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const ok = await pickUpPrewarmDayBase({
    category: "ban-ei",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm-pickup] missing watermark category=ban-ei runYmd=20260816 reason=-",
  );
  warnSpy.mockRestore();
});

test("pickUpPrewarmDayBase returns false when rowCount is not a number", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        found: true,
        parquetBase64: "YQ==",
        parquetKey: "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
        daybaseWatermark: {
          maxDataSakuseiNengappi: "",
          rowCount: "0",
          rsPredictedAtMax: "",
          rsRowCount: 0,
        },
      }),
      { status: 200 },
    ),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const ok = await pickUpPrewarmDayBase({
    category: "ban-ei",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm-pickup] missing watermark category=ban-ei runYmd=20260816 reason=-",
  );
  warnSpy.mockRestore();
});

test("pickUpPrewarmDayBase returns false when rsPredictedAtMax is not a string", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        found: true,
        parquetBase64: "YQ==",
        parquetKey: "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
        daybaseWatermark: {
          maxDataSakuseiNengappi: "",
          rowCount: 0,
          rsPredictedAtMax: null,
          rsRowCount: 0,
        },
      }),
      { status: 200 },
    ),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const ok = await pickUpPrewarmDayBase({
    category: "ban-ei",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm-pickup] missing watermark category=ban-ei runYmd=20260816 reason=-",
  );
  warnSpy.mockRestore();
});

test("pickUpPrewarmDayBase returns false when rsRowCount is not a number", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        found: true,
        parquetBase64: "YQ==",
        parquetKey: "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
        daybaseWatermark: {
          maxDataSakuseiNengappi: "",
          rowCount: 0,
          rsPredictedAtMax: "",
          rsRowCount: "0",
        },
      }),
      { status: 200 },
    ),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const ok = await pickUpPrewarmDayBase({
    category: "ban-ei",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm-pickup] missing watermark category=ban-ei runYmd=20260816 reason=-",
  );
  warnSpy.mockRestore();
});

test("pickUpPrewarmDayBase returns false when the proxy write rejects", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        found: true,
        parquetBase64: "YQ==",
        parquetKey: "feat-daybase/catalog-v1/jra/20260816/features.parquet",
        daybaseWatermark: {
          maxDataSakuseiNengappi: "20260816",
          rowCount: 12,
          rsPredictedAtMax: "2026-08-16T00:00:00",
          rsRowCount: 2,
        },
      }),
      { status: 200 },
    ),
  );
  proxyResultParquetsToRMock.mockRejectedValueOnce(new Error("r2 put failed"));
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const ok = await pickUpPrewarmDayBase({
    category: "jra",
    env: createPickupEnv({ head: headMock }),
    runYmd: "20260816",
  });
  expect(ok).toBe(false);
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm-pickup] failed category=jra runYmd=20260816 doName=predict-jra: Error: r2 put failed",
  );
  warnSpy.mockRestore();
});

test("pickUpPrewarmDayBase does not cold-start race shards after an empty day-base owner", async () => {
  stubFetchMock.mockResolvedValueOnce(
    new Response(JSON.stringify({ found: false }), { status: 200 }),
  );
  stubFetchMock.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        found: true,
        parquetBase64: "YQ==",
        parquetKey: "feat-daybase/catalog-v1/ban-ei/20260817/features.parquet",
        daybaseWatermark: {
          maxDataSakuseiNengappi: "20260817",
          rowCount: 80,
          rsPredictedAtMax: "none",
          rsRowCount: 0,
        },
      }),
      { status: 200 },
    ),
  );
  const env: Env = {
    ...createPickupEnv({ head: headMock }),
    RACE_SHARDED_DO: "1",
  };
  const ok = await pickUpPrewarmDayBase({
    category: "ban-ei",
    env,
    runYmd: "20260817",
  });
  expect(ok).toBe(false);
  expect(idFromNameMock).toHaveBeenCalledWith("predict-ban-ei");
  expect(idFromNameMock).not.toHaveBeenCalledWith("predict-ban-ei-0");
  expect(idFromNameMock).not.toHaveBeenCalledWith("predict-ban-ei-1");
  expect(proxyResultParquetsToRMock).not.toHaveBeenCalled();
});
