// Run with bun. Tests for canonical focused-full day-base readiness and repair single-flight.

import { beforeEach, expect, test, vi } from "vitest";
import type { Env } from "./types";
import {
  getDayBaseCandidateReadiness,
  getFocusedFullDayBaseReadiness,
} from "./focused-full-day-base-readiness";
import {
  clearDayBaseRepairReservation,
  DAY_BASE_REPAIR_LEASE_TTL_MS,
  enqueueDayBaseRepairOnce,
} from "./day-base-repair";

const featureHeadMock = vi.fn<() => Promise<R2Object | null>>();
const catalogFetchMock = vi.fn<(request: Request) => Promise<Response>>();
const runningStyleFirstMock = vi.fn<() => Promise<Record<string, unknown> | null>>();
const raceSourceAllMock = vi.fn<() => Promise<{ results: Record<string, unknown>[] }>>();
const runningStyleReadinessAllMock = vi.fn<() => Promise<{ results: Record<string, unknown>[] }>>();
const realtimePrepareSqlMock = vi.fn<(sql: string) => void>();
const repairFirstMock = vi.fn<() => Promise<{ category: string } | null>>();
const repairInsertBindMock = vi.fn((..._values: unknown[]) => ({ first: repairFirstMock }));
const repairPrepareSqlMock = vi.fn<(sql: string) => void>();
const repairRunMock = vi.fn(async () => ({ success: true }));
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
          : { bind: vi.fn(() => ({ first: runningStyleFirstMock, run: repairRunMock })) };
      }),
    },
    PC_KEIBA_R2_CATALOG: { fetch: catalogFetchMock },
    PREDICT_QUEUE: { send: queueSendMock },
    REALTIME_DB: {
      prepare: vi.fn((sql: string) => {
        realtimePrepareSqlMock(sql);
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
  repairFirstMock.mockReset();
  realtimePrepareSqlMock.mockClear();
  repairFirstMock.mockResolvedValue({ category: "jra" });
  repairInsertBindMock.mockClear();
  repairPrepareSqlMock.mockClear();
  repairRunMock.mockClear();
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

test("accepts a NAR artifact only after every exact-category race is complete", async () => {
  await expect(
    getFocusedFullDayBaseReadiness({ category: "nar", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: true, reason: "ready" });

  expect(runningStyleReadinessAllMock).toHaveBeenCalledTimes(1);
  expect(realtimePrepareSqlMock).toHaveBeenCalledWith(
    expect.stringContaining("races.source = 'nar' and races.keibajo_code <> '83'"),
  );
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

test("rejects stale running-style count and timestamp metadata", async () => {
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

test("rejects a stale populated artifact while the export source has no running-style rows", async () => {
  runningStyleFirstMock.mockResolvedValueOnce({
    ...readyRunningStyleRow(),
    rs_predicted_at_max: null,
    rs_row_count: 0,
  });

  await expect(
    getFocusedFullDayBaseReadiness({ category: "nar", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: false, reason: "rs-row-count-2-of-0" });
});

test("rejects a zero-running-style artifact while a category race lacks inference state", async () => {
  featureHeadMock.mockResolvedValueOnce(
    metadataObject({ "rs-predicted-at-max": "none", "rs-row-count": "0" }),
  );
  runningStyleFirstMock.mockResolvedValueOnce({
    ...readyRunningStyleRow(),
    rs_predicted_at_max: null,
    rs_row_count: 0,
  });
  runningStyleReadinessAllMock.mockResolvedValueOnce({ results: [] });

  await expect(
    getFocusedFullDayBaseReadiness({ category: "nar", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({
    ready: false,
    reason: "running-style-race-incomplete-43-01-state-missing",
  });
});

test("rejects a partial category artifact while a later race is still pending", async () => {
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
  ).resolves.toStrictEqual({
    ready: false,
    reason: "running-style-race-incomplete-43-02-state-missing",
  });
});

test("rejects a category race whose inference state is not completed", async () => {
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
  ).resolves.toStrictEqual({
    ready: false,
    reason: "running-style-race-incomplete-43-01-status-processing",
  });
});

test("rejects a category race whose prediction count is incomplete", async () => {
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
  ).resolves.toStrictEqual({
    ready: false,
    reason: "running-style-race-incomplete-43-01-prediction-count-1-of-2",
  });
});

test("fails closed when category race enumeration is empty or unavailable", async () => {
  raceSourceAllMock.mockResolvedValueOnce({
    results: [{ keibajo_code: "43", race_bango: "01", source: "nar" }],
  });
  await expect(
    getFocusedFullDayBaseReadiness({ category: "jra", env: makeEnv(), runYmd: "20260823" }),
  ).resolves.toStrictEqual({ ready: false, reason: "running-style-races-missing" });

  raceSourceAllMock.mockRejectedValueOnce(new Error("D1 enumeration unavailable"));
  await expect(
    getFocusedFullDayBaseReadiness({ category: "jra", env: makeEnv(), runYmd: "20260823" }),
  ).rejects.toThrow("D1 enumeration unavailable");
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

test("atomically enqueues one day-base repair and suppresses duplicates", async () => {
  const env = makeEnv();

  await expect(
    enqueueDayBaseRepairOnce({ category: "jra", env, runYmd: "20260823" }),
  ).resolves.toBe("enqueued");
  repairFirstMock.mockResolvedValueOnce(null);
  await expect(
    enqueueDayBaseRepairOnce({ category: "jra", env, runYmd: "20260823" }),
  ).resolves.toBe("already-enqueued");

  expect(queueSendMock).toHaveBeenCalledTimes(1);
  expect(queueSendMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 0,
    requestedAt: expect.any(String),
    runYmd: "20260823",
    type: "day-base-prewarm",
  });
});

test("preserves force on an old-date day-base repair message", async () => {
  await expect(
    enqueueDayBaseRepairOnce({
      category: "jra",
      env: makeEnv(),
      force: true,
      runYmd: "20260823",
    }),
  ).resolves.toBe("enqueued");

  expect(queueSendMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 0,
    force: true,
    requestedAt: expect.any(String),
    runYmd: "20260823",
    type: "day-base-prewarm",
  });
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
    "2026-08-23T09:15:00.000Z",
  );
  expect(repairPrepareSqlMock).toHaveBeenCalledWith(
    expect.stringContaining("where finish_position_day_base_repair_requests.requested_at <= ?4"),
  );
  expect(queueSendMock).toHaveBeenCalledTimes(1);
});

test("releases the repair reservation when Queue send fails", async () => {
  queueSendMock.mockRejectedValueOnce(new Error("Queue unavailable"));

  await expect(
    enqueueDayBaseRepairOnce({ category: "nar", env: makeEnv(), runYmd: "20260823" }),
  ).rejects.toThrow("Queue unavailable");
  expect(repairRunMock).toHaveBeenCalledTimes(1);
});

test("clears a completed repair reservation", async () => {
  await clearDayBaseRepairReservation({
    category: "jra",
    env: makeEnv(),
    runYmd: "20260823",
  });

  expect(repairRunMock).toHaveBeenCalledTimes(1);
});
