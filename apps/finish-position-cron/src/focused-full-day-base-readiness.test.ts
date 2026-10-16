// Run with bun. Tests for canonical focused-full day-base readiness and repair single-flight.

import { beforeEach, expect, test, vi } from "vitest";
import type { Env } from "./types";
import {
  getDayBaseCandidateReadiness,
  getFocusedFullDayBaseReadiness,
} from "./focused-full-day-base-readiness";
import { computeRunningStyleContentFingerprint } from "./running-style-content-hash";
import {
  acquireDayBaseRepair,
  clearDayBaseRepairReservation,
  DAY_BASE_REPAIR_LEASE_TTL_MS,
  heartbeatDayBaseRepair,
  enqueueDayBaseRepairOnce,
} from "./day-base-repair";

const featureHeadMock = vi.fn<() => Promise<R2Object | null>>();
const getDayBaseSourceMock = vi.hoisted(() =>
  vi.fn(async (params: { category: string; env: Env; runYmd: string }) => {
    const key = `feat-daybase/catalog-v1/${params.category}/${params.runYmd}/features.parquet`;
    const object = await params.env.FEATURES_CACHE.head(key);
    return object === null ? null : { key, object };
  }),
);

vi.mock("./day-base-manifest", () => ({ getDayBaseSource: getDayBaseSourceMock }));
const catalogFetchMock = vi.fn<(request: Request) => Promise<Response>>();
const runningStyleFirstMock = vi.fn<() => Promise<Record<string, unknown> | null>>();
const raceSourceAllMock = vi.fn<() => Promise<{ results: Record<string, unknown>[] }>>();
const runningStyleReadinessAllMock = vi.fn<() => Promise<{ results: Record<string, unknown>[] }>>();
const runningStyleHashAllMock =
  vi.fn<(include: string) => Promise<{ results: Record<string, unknown>[] }>>();
const realtimePrepareSqlMock = vi.fn<(sql: string) => void>();
const repairFirstMock =
  vi.fn<(generationId: string) => Promise<{ repair_generation_id: string } | null>>();
const repairOwnerFirstMock = vi.fn<() => Promise<{ repair_generation_id: string } | null>>();
const repairOwnerBindMock = vi.fn((..._values: unknown[]) => ({ first: repairOwnerFirstMock }));
const repairInsertBindMock = vi.fn(
  (
    _category: string,
    _runYmd: string,
    _requestedAt: string,
    generationId: string,
    _force: number,
    _staleBefore: string,
  ) => ({ first: () => repairFirstMock(generationId) }),
);
const repairDeleteBindMock = vi.fn((..._values: unknown[]) => ({ run: repairRunMock }));
const repairPrepareSqlMock = vi.fn<(sql: string) => void>();
const repairRunMock = vi.fn(async () => ({ success: true, meta: { changes: 1 } }));
const queueSendMock = vi.fn(async () => undefined);
const RACE_ENUMERATION_QUERY_FRAGMENT: string = "SELECT DISTINCT source";

const bindRealtimeSql = (sql: string): Record<string, unknown> => {
  if (sql.includes(RACE_ENUMERATION_QUERY_FRAGMENT)) return { all: raceSourceAllMock };
  return sql.startsWith("with target(running_key")
    ? { all: runningStyleReadinessAllMock }
    : { first: runningStyleFirstMock };
};

const metadataObject = (overrides: Record<string, string> = {}): R2Object =>
  ({
    customMetadata: {
      "max-data-sakusei-nengappi": "20260823",
      "row-count": "2",
      "rs-predicted-at-max": "2026-08-23T00:05:00.000Z",
      "rs-row-count": "2",
      ...overrides,
    },
  }) as unknown as R2Object;

const makeEnv = (): Env =>
  ({
    FEATURES_CACHE: { head: featureHeadMock },
    FINISH_POSITION_CRON_DB: {
      prepare: vi.fn((sql: string) => {
        repairPrepareSqlMock(sql);
        return sql.startsWith("insert")
          ? { bind: repairInsertBindMock }
          : sql.startsWith("delete")
            ? { bind: repairDeleteBindMock }
            : sql.includes("select repair_generation_id")
              ? { bind: repairOwnerBindMock }
              : { bind: vi.fn(() => ({ first: runningStyleFirstMock, run: repairRunMock })) };
      }),
    },
    PC_KEIBA_R2_CATALOG: { fetch: catalogFetchMock },
    PREDICT_QUEUE: { send: queueSendMock },
    REALTIME_DB: {
      prepare: vi.fn((sql: string) => {
        realtimePrepareSqlMock(sql);
        if (sql.includes("select race_key, horse_number")) {
          return {
            bind: vi.fn((include: string) => ({
              all: () => runningStyleHashAllMock(include),
            })),
          };
        }
        return { bind: vi.fn(() => bindRealtimeSql(sql)) };
      }),
    },
  }) as unknown as Env;

const readyRunningStyleRow = (): Record<string, unknown> => ({
  race_count: 1,
  rs_predicted_at_max: "2026-08-23T00:05:00Z",
  rs_row_count: 2,
});

beforeEach(() => {
  featureHeadMock.mockReset();
  featureHeadMock.mockResolvedValue(metadataObject());
  getDayBaseSourceMock.mockReset().mockImplementation(async (params) => {
    const key = `feat-daybase/catalog-v1/${params.category}/${params.runYmd}/features.parquet`;
    const object = await params.env.FEATURES_CACHE.head(key);
    return object === null ? null : { key, object };
  });
  catalogFetchMock.mockReset();
  catalogFetchMock.mockImplementation(async () =>
    Response.json({ rows: [{ horse: "a" }, { horse: "b" }] }),
  );
  runningStyleFirstMock.mockReset();
  runningStyleFirstMock.mockResolvedValue(readyRunningStyleRow());
  raceSourceAllMock.mockReset();
  raceSourceAllMock.mockResolvedValue({
    results: [
      {
        keibajo_code: "05",
        race_bango: "01",
        race_start_at_jst: "2026-08-23T10:00:00+09:00",
        source: "jra",
      },
      {
        keibajo_code: "43",
        race_bango: "01",
        race_start_at_jst: "2026-08-23T10:30:00+09:00",
        source: "nar",
      },
      {
        keibajo_code: "83",
        race_bango: "01",
        race_start_at_jst: "2026-08-23T10:45:00+09:00",
        source: "nar",
      },
    ],
  });
  runningStyleHashAllMock.mockReset();
  runningStyleHashAllMock.mockImplementation(async (include) => ({
    results: [
      {
        horse_number: 1,
        ketto_toroku_bango: "H1",
        p_nige: 0.1,
        p_oikomi: 0.4,
        p_sashi: 0.3,
        p_senkou: 0.2,
        predicted_at_max: "2026-08-23T00:05:00Z",
        predicted_label: "nige",
        race_key: include.startsWith("jra:") ? "jra:20260823:05:01" : "nar:20260823:43:01",
      },
      {
        horse_number: 2,
        ketto_toroku_bango: "H2",
        p_nige: 0.2,
        p_oikomi: 0.3,
        p_sashi: 0.1,
        p_senkou: 0.4,
        predicted_at_max: "2026-08-23T00:05:00Z",
        predicted_label: "senkou",
        race_key: include.startsWith("jra:") ? "jra:20260823:05:01" : "nar:20260823:43:01",
      },
    ],
  }));
  runningStyleReadinessAllMock.mockReset();
  runningStyleReadinessAllMock.mockResolvedValue({
    results: [
      {
        entrant_count: 2,
        expected_horse_count: 2,
        features_r2_key: "running-style/features.parquet",
        prediction_count: 2,
        running_key: "jra:20260823:05:01",
        status: "completed",
        written_horse_count: 2,
      },
      {
        entrant_count: 2,
        expected_horse_count: 2,
        features_r2_key: "running-style/features.parquet",
        prediction_count: 2,
        running_key: "nar:20260823:43:01",
        status: "completed",
        written_horse_count: 2,
      },
    ],
  });
  repairFirstMock
    .mockReset()
    .mockImplementation(async (generationId) => ({ repair_generation_id: generationId }));
  repairOwnerFirstMock
    .mockReset()
    .mockResolvedValue({ repair_generation_id: "repair-generation-current" });
  repairOwnerBindMock.mockClear();
  repairDeleteBindMock.mockReset();
  repairRunMock.mockReset().mockResolvedValue({ success: true, meta: { changes: 1 } });
  realtimePrepareSqlMock.mockClear();
  repairInsertBindMock.mockClear();
  repairPrepareSqlMock.mockClear();
  queueSendMock.mockReset();
  queueSendMock.mockResolvedValue(undefined);
});

test("accepts only matching Catalog and current running-style metadata", async () => {
  const env = makeEnv();

  await expect(
    getFocusedFullDayBaseReadiness({ category: "jra", env, runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: true, reason: "ready" });

  expect(featureHeadMock).toHaveBeenCalledWith(
    "feat-daybase/catalog-v1/jra/20260823/features.parquet",
  );
  const request = catalogFetchMock.mock.calls[0]?.[0];
  expect(request?.url).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/race-features?date=20260823&source=jra",
  );
});

test("rejects a pending or mismatched manifest without touching the legacy R2 alias", async () => {
  getDayBaseSourceMock.mockResolvedValueOnce(null);
  await expect(
    getFocusedFullDayBaseReadiness({ category: "nar", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: false, reason: "day-base-missing-or-invalid" });
  expect(featureHeadMock).not.toHaveBeenCalled();
});

test("accepts a NAR artifact only after the D1 running-style barrier is complete", async () => {
  await expect(
    getFocusedFullDayBaseReadiness({ category: "nar", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: true, reason: "ready" });

  expect(runningStyleReadinessAllMock).toHaveBeenCalledTimes(1);
  expect(runningStyleFirstMock).not.toHaveBeenCalled();
});

test("rejects missing and malformed day-base metadata before live probes", async () => {
  featureHeadMock.mockResolvedValueOnce(null);
  await expect(
    getFocusedFullDayBaseReadiness({ category: "jra", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: false, reason: "day-base-missing-or-invalid" });
  featureHeadMock.mockResolvedValueOnce(metadataObject({ "row-count": "26.5" }));
  await expect(
    getFocusedFullDayBaseReadiness({ category: "jra", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: false, reason: "day-base-missing-or-invalid" });
  featureHeadMock.mockResolvedValueOnce(metadataObject({ "rs-content-hash": " " }));
  await expect(
    getFocusedFullDayBaseReadiness({ category: "jra", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: false, reason: "day-base-missing-or-invalid" });

  expect(catalogFetchMock).not.toHaveBeenCalled();
});

test("rejects the observed partial 26-row source artifact", async () => {
  featureHeadMock.mockResolvedValueOnce(metadataObject({ "row-count": "26" }));

  await expect(
    getFocusedFullDayBaseReadiness({ category: "jra", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: false, reason: "source-row-count-26-of-2" });
});

test("rejects a partial in-process candidate before it can replace canonical R2", async () => {
  await expect(
    getDayBaseCandidateReadiness({
      category: "jra",
      env: makeEnv(),
      runYmd: "20260823",
      watermark: {
        maxDataSakuseiNengappi: "20260823",
        rowCount: 26,
        rsPredictedAtMax: "2026-08-23T00:05:00Z",
        rsRowCount: 2,
      },
    }),
  ).resolves.toStrictEqual({ ready: false, reason: "source-row-count-26-of-2" });
  expect(featureHeadMock).not.toHaveBeenCalled();
});

test("rejects malformed in-process candidate watermarks before live probes", async () => {
  const base = {
    maxDataSakuseiNengappi: "20260823",
    rowCount: 2,
    rsPredictedAtMax: "2026-08-23T00:05:00Z",
    rsRowCount: 2,
  };
  for (const watermark of [
    { ...base, maxDataSakuseiNengappi: "" },
    { ...base, rsContentHash: "" },
    { ...base, rsPredictedAtMax: "" },
    { ...base, rowCount: 2.5 },
    { ...base, rowCount: 0 },
    { ...base, rsRowCount: 1.5 },
    { ...base, rsRowCount: -1 },
  ]) {
    await expect(
      getDayBaseCandidateReadiness({
        category: "jra",
        env: makeEnv(),
        runYmd: "20260823",
        watermark,
      }),
    ).resolves.toStrictEqual({ ready: false, reason: "day-base-missing-or-invalid" });
  }
  expect(catalogFetchMock).not.toHaveBeenCalled();
  expect(runningStyleFirstMock).not.toHaveBeenCalled();
});

test("accepts a pre-RS foundation after live running-style rows complete", async () => {
  featureHeadMock.mockResolvedValueOnce(
    metadataObject({ "rs-predicted-at-max": "none", "rs-row-count": "0" }),
  );
  await expect(
    getFocusedFullDayBaseReadiness({ category: "jra", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: true, reason: "ready" });
});

test("rejects a canonical artifact from an older running-style generation", async () => {
  featureHeadMock.mockResolvedValueOnce(metadataObject({ "rs-row-count": "1" }));
  await expect(
    getFocusedFullDayBaseReadiness({ category: "jra", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: false, reason: "rs-row-count-1-of-2" });
  featureHeadMock.mockResolvedValueOnce(
    metadataObject({ "rs-predicted-at-max": "2026-08-22T23:00:00Z" }),
  );
  await expect(
    getFocusedFullDayBaseReadiness({ category: "jra", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: false, reason: "rs-predicted-at-max-mismatch" });
});

test("uses content identity instead of predicted_at for new running-style artifacts", async () => {
  const env = makeEnv();
  const fingerprint = await computeRunningStyleContentFingerprint({
    category: "jra",
    db: env.REALTIME_DB,
    runYmd: "20260823",
  });
  featureHeadMock.mockResolvedValueOnce(
    metadataObject({
      "rs-content-hash": fingerprint.contentHash,
      "rs-predicted-at-max": "2026-08-22T23:00:00Z",
    }),
  );

  await expect(
    getFocusedFullDayBaseReadiness({ category: "jra", env, runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: true, reason: "ready" });

  featureHeadMock.mockResolvedValueOnce(metadataObject({ "rs-content-hash": "stale-hash" }));
  await expect(
    getFocusedFullDayBaseReadiness({ category: "jra", env, runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: false, reason: "rs-content-hash-mismatch" });
});

test("compares a live source watermark when the Catalog projection provides it", async () => {
  catalogFetchMock.mockResolvedValueOnce(
    Response.json({
      rows: [{ data_sakusei_nengappi: "20260822" }, { data_sakusei_nengappi: "20260824" }],
    }),
  );

  await expect(
    getFocusedFullDayBaseReadiness({ category: "jra", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: false, reason: "source-watermark-mismatch" });
});

test("rejects an artifact while the D1 running-style mirror is empty", async () => {
  runningStyleFirstMock.mockResolvedValueOnce({
    ...readyRunningStyleRow(),
    rs_predicted_at_max: null,
    rs_row_count: 0,
  });
  runningStyleHashAllMock.mockResolvedValueOnce({ results: [] });

  await expect(
    getFocusedFullDayBaseReadiness({ category: "nar", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: false, reason: "rs-row-count-2-of-0" });
  expect(runningStyleFirstMock).not.toHaveBeenCalled();
});

test("requires per-race D1 inference completion before artifact freshness", async () => {
  featureHeadMock.mockResolvedValueOnce(
    metadataObject({ "rs-predicted-at-max": "none", "rs-row-count": "0" }),
  );
  runningStyleFirstMock.mockResolvedValueOnce({
    ...readyRunningStyleRow(),
    rs_predicted_at_max: null,
    rs_row_count: 0,
  });
  runningStyleHashAllMock.mockResolvedValueOnce({ results: [] });
  runningStyleReadinessAllMock.mockResolvedValueOnce({ results: [] });

  await expect(
    getFocusedFullDayBaseReadiness({ category: "nar", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: false, reason: "running-style-race-count-0-of-1" });
});

test("blocks a category artifact on a later incomplete D1 race mirror", async () => {
  runningStyleFirstMock.mockResolvedValueOnce({
    ...readyRunningStyleRow(),
    race_count: 2,
  });
  raceSourceAllMock.mockResolvedValueOnce({
    results: [
      {
        keibajo_code: "43",
        race_bango: "01",
        race_start_at_jst: "2026-08-23T10:30:00+09:00",
        source: "nar",
      },
      {
        keibajo_code: "43",
        race_bango: "02",
        race_start_at_jst: "2026-08-23T11:00:00+09:00",
        source: "nar",
      },
    ],
  });
  runningStyleReadinessAllMock.mockResolvedValueOnce({
    results: [
      {
        entrant_count: 2,
        expected_horse_count: 2,
        features_r2_key: "running-style/features.parquet",
        prediction_count: 2,
        running_key: "nar:20260823:43:01",
        status: "completed",
        written_horse_count: 2,
      },
    ],
  });

  await expect(
    getFocusedFullDayBaseReadiness({ category: "nar", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: false, reason: "running-style-race-count-1-of-2" });
});

test("accepts a partial running-style race set when the artifact already holds the current rows", async () => {
  const env = makeEnv();
  const fingerprint = await computeRunningStyleContentFingerprint({
    category: "nar",
    db: env.REALTIME_DB,
    runYmd: "20260823",
  });
  featureHeadMock.mockResolvedValueOnce(
    metadataObject({ "rs-content-hash": fingerprint.contentHash }),
  );
  runningStyleFirstMock.mockResolvedValueOnce({ ...readyRunningStyleRow(), race_count: 2 });
  raceSourceAllMock.mockResolvedValueOnce({
    results: [
      {
        keibajo_code: "43",
        race_bango: "01",
        race_start_at_jst: "2026-08-23T10:30:00+09:00",
        source: "nar",
      },
      {
        keibajo_code: "43",
        race_bango: "02",
        race_start_at_jst: "2026-08-23T11:00:00+09:00",
        source: "nar",
      },
    ],
  });

  await expect(
    getFocusedFullDayBaseReadiness({ category: "nar", env, runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: true, reason: "ready" });
});

test("still rebuilds a partial running-style race set when the artifact holds older rows", async () => {
  featureHeadMock.mockResolvedValueOnce(metadataObject({ "rs-content-hash": "older-rs-rows" }));
  runningStyleFirstMock.mockResolvedValueOnce({ ...readyRunningStyleRow(), race_count: 2 });
  raceSourceAllMock.mockResolvedValueOnce({
    results: [
      {
        keibajo_code: "43",
        race_bango: "01",
        race_start_at_jst: "2026-08-23T10:30:00+09:00",
        source: "nar",
      },
      {
        keibajo_code: "43",
        race_bango: "02",
        race_start_at_jst: "2026-08-23T11:00:00+09:00",
        source: "nar",
      },
    ],
  });

  await expect(
    getFocusedFullDayBaseReadiness({ category: "nar", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: false, reason: "running-style-race-count-1-of-2" });
});

test("blocks an R2 artifact on a processing D1 mirror state", async () => {
  runningStyleReadinessAllMock.mockResolvedValueOnce({
    results: [
      {
        entrant_count: 2,
        expected_horse_count: 2,
        features_r2_key: "running-style/features.parquet",
        prediction_count: 2,
        running_key: "nar:20260823:43:01",
        status: "processing",
        written_horse_count: 2,
      },
    ],
  });

  await expect(
    getFocusedFullDayBaseReadiness({ category: "nar", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: false, reason: "running-style-race-count-0-of-1" });
});

test("blocks an R2 artifact on a short D1 prediction count", async () => {
  runningStyleReadinessAllMock.mockResolvedValueOnce({
    results: [
      {
        entrant_count: 2,
        expected_horse_count: 2,
        features_r2_key: "running-style/features.parquet",
        prediction_count: 1,
        running_key: "nar:20260823:43:01",
        status: "completed",
        written_horse_count: 2,
      },
    ],
  });

  await expect(
    getFocusedFullDayBaseReadiness({ category: "nar", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: false, reason: "running-style-race-count-0-of-1" });
});

test("fails closed when category race enumeration is unavailable", async () => {
  raceSourceAllMock.mockResolvedValueOnce({
    results: [{ keibajo_code: "43", race_bango: "01", source: "nar" }],
  });
  await expect(
    getFocusedFullDayBaseReadiness({ category: "jra", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: true, reason: "ready" });

  raceSourceAllMock.mockRejectedValueOnce(new Error("D1 enumeration unavailable"));
  await expect(
    getFocusedFullDayBaseReadiness({ category: "jra", env: makeEnv(), runYmd: "20260823" }),
  ).rejects.toThrow("D1 enumeration unavailable");
  expect(raceSourceAllMock).toHaveBeenCalledTimes(2);
});

test("uses the canonical no-running-style watermark for Ban-ei", async () => {
  featureHeadMock.mockResolvedValueOnce(
    metadataObject({ "rs-predicted-at-max": "none", "rs-row-count": "0" }),
  );

  await expect(
    getFocusedFullDayBaseReadiness({ category: "ban-ei", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: true, reason: "ready" });
  expect(runningStyleFirstMock).not.toHaveBeenCalled();
});

test("fails closed when Catalog readiness cannot be established", async () => {
  catalogFetchMock.mockResolvedValueOnce(new Response("unavailable", { status: 503 }));
  await expect(
    getFocusedFullDayBaseReadiness({ category: "jra", env: makeEnv(), runYmd: "20260823" }),
  ).rejects.toThrow("Catalog day-base readiness failed with HTTP 503");
  const env = { ...makeEnv(), PC_KEIBA_R2_CATALOG: undefined };
  await expect(
    getFocusedFullDayBaseReadiness({ category: "jra", env, runYmd: "20260823" }),
  ).rejects.toThrow("PC_KEIBA_R2_CATALOG binding is unavailable");
});

test("acquires and reuses the supplied day-base owner generation", async () => {
  repairFirstMock.mockResolvedValue({ repair_generation_id: "workflow-generation-1" });
  const env = makeEnv();

  await expect(
    acquireDayBaseRepair({
      category: "jra",
      env,
      generationId: "workflow-generation-1",
      runYmd: "20990101",
    }),
  ).resolves.toStrictEqual({
    generationId: "workflow-generation-1",
    status: "acquired",
  });
  await expect(
    acquireDayBaseRepair({
      category: "jra",
      env,
      generationId: "workflow-generation-1",
      runYmd: "20990101",
    }),
  ).resolves.toStrictEqual({
    generationId: "workflow-generation-1",
    status: "acquired",
  });

  expect(repairInsertBindMock).toHaveBeenNthCalledWith(
    1,
    "jra",
    "20990101",
    expect.any(String),
    "workflow-generation-1",
    0,
    expect.any(String),
  );
  expect(repairInsertBindMock).toHaveBeenNthCalledWith(
    2,
    "jra",
    "20990101",
    expect.any(String),
    "workflow-generation-1",
    0,
    expect.any(String),
  );
  expect(queueSendMock).not.toHaveBeenCalled();
});

test("reports the current owner when another generation holds a live day-base lease", async () => {
  repairFirstMock.mockResolvedValueOnce(null);

  await expect(
    acquireDayBaseRepair({
      category: "nar",
      env: makeEnv(),
      generationId: "workflow-generation-new",
      runYmd: "20990101",
    }),
  ).resolves.toStrictEqual({
    generationId: "repair-generation-current",
    status: "busy",
  });

  expect(repairOwnerBindMock).toHaveBeenCalledWith("nar", "20990101");
});

test("reports a busy lease without an owner when it is cleared during acquisition", async () => {
  repairFirstMock.mockResolvedValueOnce(null);
  repairOwnerFirstMock.mockResolvedValueOnce(null);

  await expect(
    acquireDayBaseRepair({
      category: "nar",
      env: makeEnv(),
      generationId: "workflow-generation-new",
      runYmd: "20990101",
    }),
  ).resolves.toStrictEqual({ generationId: null, status: "busy" });
});

test("heartbeats only the matching live owner generation", async () => {
  const now = new Date("2026-08-23T10:00:00.000Z");
  const env = makeEnv();

  await expect(
    heartbeatDayBaseRepair({
      category: "nar",
      env,
      generationId: "repair-generation-1",
      now,
      runYmd: "20260823",
    }),
  ).resolves.toBe(true);

  expect(repairPrepareSqlMock).toHaveBeenCalledWith(
    expect.stringContaining("repair_generation_id = ?3 and requested_at > ?5"),
  );
  expect(repairRunMock).toHaveBeenCalledOnce();
});

test("does not renew an expired or superseded owner generation", async () => {
  repairRunMock.mockResolvedValueOnce({ success: true, meta: { changes: 0 } });

  await expect(
    heartbeatDayBaseRepair({
      category: "jra",
      env: makeEnv(),
      generationId: "old-generation",
      now: new Date("2026-08-23T10:00:00.000Z"),
      runYmd: "20260823",
    }),
  ).resolves.toBe(false);
});

test("force acquisition takes over a live day-base owner with the supplied generation", async () => {
  const env = makeEnv();
  repairFirstMock.mockResolvedValueOnce({ repair_generation_id: "workflow-generation-forced" });

  await expect(
    acquireDayBaseRepair({
      category: "ban-ei",
      env,
      force: true,
      generationId: "workflow-generation-forced",
      now: new Date("2026-08-23T10:00:00.000Z"),
      runYmd: "20260823",
    }),
  ).resolves.toStrictEqual({
    generationId: "workflow-generation-forced",
    status: "acquired",
  });
  expect(repairInsertBindMock).toHaveBeenCalledWith(
    "ban-ei",
    "20260823",
    "2026-08-23T10:00:00.000Z",
    "workflow-generation-forced",
    1,
    "2026-08-23T09:15:00.000Z",
  );
});

test("atomically enqueues one day-base repair and suppresses duplicates", async () => {
  const env = makeEnv();

  await expect(
    enqueueDayBaseRepairOnce({ category: "jra", env, runYmd: "20990101" }),
  ).resolves.toBe("enqueued");
  repairFirstMock.mockResolvedValueOnce(null);
  await expect(
    enqueueDayBaseRepairOnce({ category: "jra", env, runYmd: "20990101" }),
  ).resolves.toBe("already-enqueued");

  expect(queueSendMock).toHaveBeenCalledTimes(1);
  expect(queueSendMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 0,
    generationId: expect.any(String),
    requestedAt: expect.any(String),
    runYmd: "20990101",
    type: "day-base-prewarm",
  });
  expect(repairInsertBindMock).toHaveBeenCalledWith(
    "jra",
    "20990101",
    expect.any(String),
    expect.any(String),
    0,
    expect.any(String),
  );
  const reservedGenerationId = repairInsertBindMock.mock.calls[0]?.[3];
  expect(queueSendMock).toHaveBeenCalledWith(
    expect.objectContaining({ generationId: reservedGenerationId }),
  );
});

test("rejects force on an old-date day-base repair message before reserving", async () => {
  await expect(
    enqueueDayBaseRepairOnce({
      category: "jra",
      env: makeEnv(),
      force: true,
      now: new Date("2026-08-24T00:00:00.000Z"),
      runYmd: "20260823",
    }),
  ).resolves.toBe("skipped-old-date");

  expect(queueSendMock).not.toHaveBeenCalled();
  expect(repairPrepareSqlMock).not.toHaveBeenCalled();
});

test("forced rebuild does not replace a live day-base owner", async () => {
  repairFirstMock.mockResolvedValueOnce(null);
  repairOwnerFirstMock.mockResolvedValueOnce({ repair_generation_id: "workflow-generation" });

  await expect(
    enqueueDayBaseRepairOnce({
      category: "nar",
      env: makeEnv(),
      force: true,
      now: new Date("2026-08-23T10:00:00.000Z"),
      runYmd: "20260823",
    }),
  ).resolves.toBe("already-enqueued");

  expect(repairInsertBindMock).toHaveBeenCalledWith(
    "nar",
    "20260823",
    "2026-08-23T10:00:00.000Z",
    expect.any(String),
    0,
    "2026-08-23T09:15:00.000Z",
  );
  expect(queueSendMock).not.toHaveBeenCalled();
});

test("forced rebuild is forwarded to the Queue without force-acquiring its reservation", async () => {
  repairFirstMock.mockResolvedValueOnce({ repair_generation_id: "repair-generation-1" });
  const now = new Date("2026-08-23T10:00:00.000Z");

  await expect(
    enqueueDayBaseRepairOnce({
      category: "nar",
      env: makeEnv(),
      force: true,
      now,
      runYmd: "20260823",
    }),
  ).resolves.toBe("enqueued");

  expect(repairInsertBindMock).toHaveBeenCalledWith(
    "nar",
    "20260823",
    "2026-08-23T10:00:00.000Z",
    expect.any(String),
    0,
    "2026-08-23T09:15:00.000Z",
  );
  expect(queueSendMock).toHaveBeenCalledWith(
    expect.objectContaining({ force: true, generationId: expect.any(String) }),
  );
});

test("atomically reclaims and enqueues a repair after the 45-minute lease expires", async () => {
  const now = new Date("2026-08-23T10:00:00.000Z");
  const env = makeEnv();

  await expect(
    enqueueDayBaseRepairOnce({ category: "jra", env, now, runYmd: "20260823" }),
  ).resolves.toBe("enqueued");

  expect(DAY_BASE_REPAIR_LEASE_TTL_MS).toBe(45 * 60 * 1000);
  expect(repairInsertBindMock).toHaveBeenCalledWith(
    "jra",
    "20260823",
    "2026-08-23T10:00:00.000Z",
    expect.any(String),
    0,
    "2026-08-23T09:15:00.000Z",
  );
  expect(repairPrepareSqlMock).toHaveBeenCalledWith(
    expect.stringContaining("finish_position_day_base_repair_requests.requested_at <= ?6"),
  );
  expect(queueSendMock).toHaveBeenCalledTimes(1);
});

test("releases the repair reservation when Queue send fails", async () => {
  queueSendMock.mockRejectedValueOnce(new Error("Queue unavailable"));

  await expect(
    enqueueDayBaseRepairOnce({ category: "nar", env: makeEnv(), runYmd: "20990101" }),
  ).rejects.toThrow("Queue unavailable");
  expect(repairRunMock).toHaveBeenCalledTimes(1);
  expect(repairDeleteBindMock).toHaveBeenCalledWith("nar", "20990101", expect.any(String));
});

test("clears only the reservation owned by the matching generation", async () => {
  const env = makeEnv();

  await expect(
    clearDayBaseRepairReservation({
      category: "jra",
      env,
      generationId: "repair-generation-1",
      runYmd: "20260823",
    }),
  ).resolves.toBe(true);

  expect(repairPrepareSqlMock).toHaveBeenCalledWith(
    expect.stringContaining("and repair_generation_id = ?3"),
  );
  expect(repairDeleteBindMock).toHaveBeenCalledWith("jra", "20260823", "repair-generation-1");
  expect(repairRunMock).toHaveBeenCalledTimes(1);
});

test("does not clear a legacy reservation without an owner generation", async () => {
  await expect(
    clearDayBaseRepairReservation({
      category: "jra",
      env: makeEnv(),
      runYmd: "20260823",
    }),
  ).resolves.toBe(false);

  expect(repairDeleteBindMock).not.toHaveBeenCalled();
  expect(repairRunMock).not.toHaveBeenCalled();
});
