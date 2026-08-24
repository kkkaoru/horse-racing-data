import { beforeEach, expect, it, vi } from "vitest";

const queryMock = vi.fn();
vi.mock("@neondatabase/serverless", () => ({
  neon: vi.fn(() => ({ query: queryMock })),
}));

import { buildPredictionReadiness, getPredictionReadiness } from "./prediction-readiness";
import type { Env } from "./types";

const NOW = new Date("2026-08-15T00:00:00Z");

beforeEach(() => {
  queryMock.mockReset();
});

it("builds race coverage by intersecting eligible entries and predictions", () => {
  const result = buildPredictionReadiness({
    entries: [
      { ketto_toroku_bango: "H1", keibajo_code: "5", race_bango: "1", source: "jra" },
      { ketto_toroku_bango: "H2", keibajo_code: "5", race_bango: "1", source: "jra" },
    ],
    now: NOW,
    predictions: [
      {
        generated_at: "2026-08-15T00:00:00Z",
        ketto_toroku_bango: "H1",
        keibajo_code: "05",
        race_bango: "01",
        source: "jra",
      },
    ],
    races: [
      {
        keibajo_code: "5",
        race_bango: "1",
        race_start_at_jst: "2026-08-15T10:00:00+09:00",
        source: "jra",
      },
    ],
    runYmd: "20260815",
  });
  expect(result.races).toHaveLength(1);
  expect(result.races[0]).toMatchObject({
    complete: false,
    deadline: "T-60",
    expectedCount: 2,
    missingCount: 1,
    predictionCount: 1,
    raceKey: "jra:05:01",
  });
});

it("classifies T-120, T-30, post, complete, and outside-window races", () => {
  const race = (raceBango: string, start: string) => ({
    keibajo_code: "05",
    race_bango: raceBango,
    race_start_at_jst: start,
    source: "jra",
  });
  const entries = ["01", "02", "03", "04", "05"].map((raceBango) => ({
    ketto_toroku_bango: `H${raceBango}`,
    keibajo_code: "05",
    race_bango: raceBango,
    source: "jra",
  }));
  const predictions = entries.map((entry) => ({
    generated_at: "2026-08-15T00:00:00Z",
    ...entry,
  }));
  const result = buildPredictionReadiness({
    entries,
    now: NOW,
    predictions,
    races: [
      race("01", "2026-08-15T10:59:00+09:00"),
      race("02", "2026-08-15T09:29:00+09:00"),
      race("03", "2026-08-15T08:59:00+09:00"),
      race("04", "2026-08-13T08:20:00+09:00"),
      race("05", "invalid"),
    ],
    runYmd: "20260815",
  });
  expect(result.races.map((item) => item.deadline)).toEqual(["T-120", "T-30", "post"]);
  expect(result.races.every((item) => item.complete)).toBe(true);
});

it("keeps races earlier than T-120 visible so all future races are diagnosable", () => {
  const result = buildPredictionReadiness({
    entries: [
      {
        ketto_toroku_bango: "H1",
        keibajo_code: "46",
        race_bango: "01",
        source: "nar",
        umaban: 1,
      },
    ],
    now: new Date("2026-08-23T21:36:00Z"),
    predictions: [],
    races: [
      {
        keibajo_code: "46",
        race_bango: "01",
        race_start_at_jst: "2026-08-24T11:45:00+09:00",
        source: "nar",
      },
    ],
    runYmd: "20260824",
  });

  expect(result.races).toHaveLength(1);
  expect(result.races[0]).toMatchObject({
    deadline: "T-120",
    minutesToPost: 309,
    raceKey: "nar:46:01",
    started: false,
  });
  expect(result.summary).toMatchObject({
    notStartedRaceCount: 1,
    raceCount: 1,
  });
});

it("reports missing entry and prediction maps with null timestamps", () => {
  const result = buildPredictionReadiness({
    entries: [],
    now: NOW,
    predictions: [],
    races: [
      {
        keibajo_code: "05",
        race_bango: "01",
        race_start_at_jst: "2026-08-15T10:00:00+09:00",
        source: "jra",
      },
    ],
    runYmd: "20260815",
  });
  expect(result.races[0]).toMatchObject({
    complete: false,
    expectedCount: 0,
    newestPredictionAt: null,
    oldestPredictionAt: null,
    predictionCount: 0,
  });
});

it("keeps the newest prediction timestamp when multiple expected horses match", () => {
  const result = buildPredictionReadiness({
    entries: ["H1", "H2"].map((id) => ({
      ketto_toroku_bango: id,
      keibajo_code: "05",
      race_bango: "01",
      source: "jra",
    })),
    now: NOW,
    predictions: ["H1", "H2"].map((id, index) => ({
      generated_at: `2026-08-15T00:0${index}:00Z`,
      ketto_toroku_bango: id,
      keibajo_code: "05",
      race_bango: "01",
      source: "jra",
    })),
    races: [
      {
        keibajo_code: "05",
        race_bango: "01",
        race_start_at_jst: "2026-08-15T10:00:00+09:00",
        source: "jra",
      },
    ],
    runYmd: "20260815",
  });
  expect(result.races[0]).toMatchObject({
    complete: true,
    newestPredictionAt: "2026-08-15T00:01:00Z",
    oldestPredictionAt: "2026-08-15T00:00:00Z",
  });
});

it("loads race, entry, and prediction rows in batches and filters placeholder IDs", async () => {
  const allMock = vi
    .fn()
    .mockResolvedValueOnce({
      results: [
        {
          keibajo_code: "05",
          race_bango: "01",
          race_start_at_jst: "2026-08-15T10:00:00+09:00",
          source: "jra",
        },
      ],
    })
    .mockResolvedValueOnce({
      results: [
        { ketto_toroku_bango: "H1", keibajo_code: "05", race_bango: "01", source: "jra" },
        {
          ketto_toroku_bango: "0000000000",
          keibajo_code: "05",
          race_bango: "01",
          source: "jra",
        },
      ],
    })
    .mockResolvedValueOnce({ results: [] });
  const bindMock = vi.fn(() => ({ all: allMock }));
  const prepareMock = vi.fn((_sql: string) => ({ bind: bindMock }));
  queryMock.mockResolvedValue([
    {
      generated_at: "2026-08-15T00:00:00Z",
      ketto_toroku_bango: "H1",
      keibajo_code: "05",
      race_bango: "01",
      source: "jra",
    },
  ]);
  const env = {
    NEON_DATABASE_URL: "postgres://example",
    REALTIME_DB: { prepare: prepareMock },
  } as unknown as Env;
  const result = await getPredictionReadiness({ env, now: NOW, runYmd: "20260815" });
  expect(result.races[0]).toMatchObject({ complete: true, expectedCount: 1 });
  expect(prepareMock).toHaveBeenCalledTimes(3);
  expect(queryMock).toHaveBeenCalledTimes(1);
  const preparedSql = prepareMock.mock.calls.map(([sql]) => String(sql)).join("\n");
  expect(preparedSql).toContain("max(entries.fetched_at)");
  expect(preparedSql).toContain("entries.horse_number as umaban");
  const predictionSql = String(queryMock.mock.calls[0]);
  expect(predictionSql).toContain("ketto_toroku_bango, umaban");
  expect(predictionSql).toContain("select distinct on");
  expect(predictionSql).toContain("prediction_generated_at desc");
  expect(predictionSql).not.toContain("prediction_generated_at >=");
  expect(queryMock.mock.calls[0]?.[1]).toStrictEqual(["2026", "0815"]);
});

it("falls back per race to active horses in the latest entry snapshot and matches by umaban", async () => {
  const allMock = vi
    .fn()
    .mockResolvedValueOnce({
      results: [
        {
          keibajo_code: "55",
          race_bango: "03",
          race_start_at_jst: "2026-08-15T10:00:00+09:00",
          source: "nar",
        },
        {
          keibajo_code: "55",
          race_bango: "04",
          race_start_at_jst: "2026-08-15T10:10:00+09:00",
          source: "nar",
        },
      ],
    })
    .mockResolvedValueOnce({
      results: [
        {
          ketto_toroku_bango: "H1",
          keibajo_code: "55",
          race_bango: "03",
          source: "nar",
          umaban: "01",
        },
        {
          ketto_toroku_bango: "H4",
          keibajo_code: "55",
          race_bango: "04",
          source: "nar",
          umaban: "04",
        },
      ],
    })
    .mockResolvedValueOnce({
      results: [
        { keibajo_code: "55", race_bango: "03", source: "nar", status: null, umaban: "01" },
        { keibajo_code: "55", race_bango: "03", source: "nar", status: null, umaban: "02" },
        {
          keibajo_code: "55",
          race_bango: "03",
          source: "nar",
          status: "出走取消",
          umaban: "03",
        },
        { keibajo_code: "55", race_bango: "03", source: "nar", status: null, umaban: "00" },
        { keibajo_code: "55", race_bango: "04", source: "nar", status: null, umaban: "4" },
      ],
    });
  const prepareMock = vi.fn((_sql: string) => ({ bind: vi.fn(() => ({ all: allMock })) }));
  queryMock.mockResolvedValue([
    {
      generated_at: "2026-08-15T00:00:00Z",
      ketto_toroku_bango: "H1",
      keibajo_code: "55",
      race_bango: "03",
      source: "nar",
      umaban: 1,
    },
    {
      generated_at: "2026-08-15T00:01:00Z",
      ketto_toroku_bango: "H4",
      keibajo_code: "55",
      race_bango: "04",
      source: "nar",
      umaban: 4,
    },
  ]);
  const env = {
    NEON_DATABASE_URL: "postgres://example",
    REALTIME_DB: { prepare: prepareMock },
  } as unknown as Env;

  const result = await getPredictionReadiness({ env, now: NOW, runYmd: "20260815" });

  expect(result.races[0]).toMatchObject({
    complete: false,
    expectedCount: 2,
    missingCount: 1,
    predictionCount: 1,
  });
  expect(result.races[1]).toMatchObject({ complete: true, expectedCount: 1 });
});

it("treats a non-array Neon response as no predictions", async () => {
  const allMock = vi.fn().mockResolvedValue({ results: [] });
  const env = {
    NEON_DATABASE_URL: "postgres://example",
    REALTIME_DB: { prepare: vi.fn(() => ({ bind: vi.fn(() => ({ all: allMock })) })) },
  } as unknown as Env;
  queryMock.mockResolvedValue({ rows: [] });
  const result = await getPredictionReadiness({ env, now: NOW, runYmd: "20260815" });
  expect(result.races).toEqual([]);
});

it("separates pre-weight display readiness from a complete post-weight generation", () => {
  const result = buildPredictionReadiness({
    entries: [
      {
        ketto_toroku_bango: "H1",
        keibajo_code: "05",
        race_bango: "01",
        source: "jra",
        umaban: "1",
      },
      {
        ketto_toroku_bango: "H2",
        keibajo_code: "05",
        race_bango: "01",
        source: "jra",
        umaban: "2",
      },
    ],
    kvPayloads: new Map([
      [
        "jra:05:01",
        [
          {
            horseNumber: "01",
            modelVersion: "model-v2",
            predictionGeneratedAt: "2026-08-15T00:06:00.000Z",
          },
          {
            horseNumber: "02",
            modelVersion: "model-v2",
            predictionGeneratedAt: "2026-08-15T00:06:00.000Z",
          },
        ],
      ],
    ]),
    now: NOW,
    predictions: [
      {
        generated_at: "2026-08-15T00:06:00Z",
        ketto_toroku_bango: "H1",
        keibajo_code: "05",
        model_version: "model-v2",
        race_bango: "01",
        source: "jra",
        umaban: 1,
      },
      {
        generated_at: "2026-08-15T00:06:00Z",
        ketto_toroku_bango: "H2",
        keibajo_code: "05",
        model_version: "model-v2",
        race_bango: "01",
        source: "jra",
        umaban: 2,
      },
    ],
    races: [
      {
        keibajo_code: "05",
        last_weight_fetch_at: "2026-08-15T00:05:30Z",
        race_bango: "01",
        race_start_at_jst: "2026-08-15T10:00:00+09:00",
        source: "jra",
        weight_snapshot_at: "2026-08-15T00:05:00Z",
        weight_snapshot_count: "2",
      },
    ],
    runYmd: "20260815",
  });

  expect(result.races[0]?.preWeight).toStrictEqual({
    complete: true,
    kvComplete: true,
    kvGenerationMatchesNeon: true,
    kvPredictionCount: 2,
    kvSingleGeneration: true,
    missingCount: 0,
    neonComplete: true,
    newestPredictionAt: "2026-08-15T00:06:00Z",
    oldestPredictionAt: "2026-08-15T00:06:00Z",
    predictionCount: 2,
    reason: null,
  });
  expect(result.races[0]?.postWeight).toStrictEqual({
    complete: true,
    kvComplete: true,
    kvAfterWeight: true,
    kvGenerationMatchesNeon: true,
    kvPredictionCount: 2,
    kvSingleGeneration: true,
    lastWeightFetchAt: "2026-08-15T00:05:30Z",
    missingCount: 0,
    neonComplete: true,
    newestPredictionAt: "2026-08-15T00:06:00Z",
    oldestPredictionAt: "2026-08-15T00:06:00Z",
    predictionAfterWeightCount: 2,
    predictionCount: 2,
    reason: null,
    status: "complete",
    weightSnapshotAt: "2026-08-15T00:05:00Z",
    weightSnapshotCount: 2,
    weightReady: true,
  });
  expect(result.summary).toStrictEqual({
    notStartedRaceCount: 1,
    postWeightCompleteRaceCount: 1,
    postWeightIncompleteBeforePostCount: 0,
    preWeightCompleteRaceCount: 1,
    preWeightIncompleteBeforePostCount: 0,
    raceCount: 1,
  });
});

it("reports weight delivery pending without conflating it with pre-weight completion", () => {
  const result = buildPredictionReadiness({
    entries: [
      {
        ketto_toroku_bango: "H1",
        keibajo_code: "05",
        race_bango: "01",
        source: "jra",
        umaban: 1,
      },
    ],
    kvPayloads: new Map([
      [
        "jra:05:01",
        [
          {
            horseNumber: "1",
            modelVersion: "model-v1",
            predictionGeneratedAt: "2026-08-15T00:00:00Z",
          },
        ],
      ],
    ]),
    now: NOW,
    predictions: [
      {
        generated_at: "2026-08-15T00:00:00Z",
        ketto_toroku_bango: "H1",
        keibajo_code: "05",
        model_version: "model-v1",
        race_bango: "01",
        source: "jra",
        umaban: 1,
      },
    ],
    races: [
      {
        keibajo_code: "05",
        last_weight_fetch_at: null,
        race_bango: "01",
        race_start_at_jst: "2026-08-15T10:00:00+09:00",
        source: "jra",
        weight_snapshot_at: null,
        weight_snapshot_count: 0,
      },
    ],
    runYmd: "20260815",
  });

  expect(result.races[0]?.complete).toBe(true);
  expect(result.races[0]?.preWeight.complete).toBe(true);
  expect(result.races[0]?.postWeight).toMatchObject({
    complete: false,
    predictionAfterWeightCount: 0,
    reason: "weight-not-delivered",
    status: "waiting-for-weight",
  });
  expect(result.summary).toMatchObject({
    postWeightIncompleteBeforePostCount: 1,
    preWeightIncompleteBeforePostCount: 0,
  });
});

it("requires complete snapshots and a newer uniform KV generation for post-weight readiness", () => {
  const result = buildPredictionReadiness({
    entries: [
      {
        ketto_toroku_bango: "H1",
        keibajo_code: "05",
        race_bango: "01",
        source: "jra",
        umaban: 1,
      },
      {
        ketto_toroku_bango: "H2",
        keibajo_code: "05",
        race_bango: "01",
        source: "jra",
        umaban: 2,
      },
    ],
    kvPayloads: new Map([
      [
        "jra:05:01",
        [
          {
            horseNumber: "1",
            modelVersion: "old-model",
            predictionGeneratedAt: "2026-08-15T00:04:00Z",
          },
          {
            horseNumber: "2",
            modelVersion: "model-v2",
            predictionGeneratedAt: "2026-08-15T00:06:00Z",
          },
        ],
      ],
    ]),
    now: NOW,
    predictions: [
      {
        generated_at: "2026-08-15T00:06:00Z",
        ketto_toroku_bango: "H1",
        keibajo_code: "05",
        model_version: "model-v2",
        race_bango: "01",
        source: "jra",
        umaban: 1,
      },
      {
        generated_at: "2026-08-15T00:06:00Z",
        ketto_toroku_bango: "H2",
        keibajo_code: "05",
        model_version: "model-v2",
        race_bango: "01",
        source: "jra",
        umaban: 2,
      },
    ],
    races: [
      {
        keibajo_code: "05",
        last_weight_fetch_at: "2026-08-15T00:05:30Z",
        race_bango: "01",
        race_start_at_jst: "2026-08-15T10:00:00+09:00",
        source: "jra",
        weight_snapshot_at: "2026-08-15T00:05:00Z",
        weight_snapshot_count: 1,
      },
    ],
    runYmd: "20260815",
  });

  expect(result.races[0]?.preWeight).toMatchObject({
    complete: false,
    kvComplete: false,
    kvGenerationMatchesNeon: false,
    kvSingleGeneration: false,
    reason: "kv-generation-mismatch",
  });
  expect(result.races[0]?.postWeight).toMatchObject({
    complete: false,
    reason: "weight-snapshot-incomplete",
    status: "pending",
    weightSnapshotCount: 1,
  });
});

it("loads and validates KV payloads together with D1 weight provenance", async () => {
  const allMock = vi
    .fn()
    .mockResolvedValueOnce({
      results: [
        {
          keibajo_code: "05",
          last_weight_fetch_at: "2026-08-15T00:05:00Z",
          race_bango: "01",
          race_start_at_jst: "2026-08-15T10:00:00+09:00",
          source: "jra",
          weight_snapshot_at: "2026-08-15T00:05:00Z",
          weight_snapshot_count: 1,
        },
      ],
    })
    .mockResolvedValueOnce({
      results: [
        {
          ketto_toroku_bango: "H1",
          keibajo_code: "05",
          race_bango: "01",
          source: "jra",
          umaban: "1",
        },
      ],
    })
    .mockResolvedValueOnce({ results: [] });
  const prepareMock = vi.fn((_sql: string) => ({ bind: vi.fn(() => ({ all: allMock })) }));
  const getMock = vi.fn(async () => [
    {
      horseNumber: "1",
      modelVersion: "model-v2",
      predictionGeneratedAt: "2026-08-15T00:06:00.000Z",
    },
    { horseNumber: 2, modelVersion: "invalid", predictionGeneratedAt: null },
  ]);
  queryMock.mockResolvedValue([
    {
      generated_at: "2026-08-15T00:06:00Z",
      ketto_toroku_bango: "H1",
      keibajo_code: "05",
      model_version: "model-v2",
      race_bango: "01",
      source: "jra",
      umaban: 1,
    },
  ]);
  const env = {
    DETAIL_SECTION_CACHE_KV: { get: getMock },
    NEON_DATABASE_URL: "postgres://example",
    REALTIME_DB: { prepare: prepareMock },
  } as unknown as Env;

  const result = await getPredictionReadiness({ env, now: NOW, runYmd: "20260815" });

  expect(getMock).toHaveBeenCalledWith("pred:fp:v1:20260815:05:01", "json");
  expect(String(prepareMock.mock.calls[0]?.[0])).toContain("last_weight_fetch_at");
  expect(String(prepareMock.mock.calls[0]?.[0])).toContain("horse_weight_snapshots");
  expect(result.races[0]?.postWeight).toMatchObject({
    complete: true,
    kvPredictionCount: 1,
    predictionAfterWeightCount: 1,
  });
});

it("diagnoses missing snapshots, stale predictions, stale KV, and invalid weight counts", () => {
  const result = buildPredictionReadiness({
    entries: ["01", "02", "03", "04"].map((raceBango) => ({
      ketto_toroku_bango: `H${raceBango}`,
      keibajo_code: "05",
      race_bango: raceBango,
      source: "jra",
      umaban: 1,
    })),
    kvPayloads: new Map([
      [
        "jra:05:01",
        [
          {
            horseNumber: "1",
            modelVersion: "model-v2",
            predictionGeneratedAt: "2026-08-15T00:06:00Z",
          },
        ],
      ],
      [
        "jra:05:02",
        [
          {
            horseNumber: "1",
            modelVersion: "model-v1",
            predictionGeneratedAt: "2026-08-15T00:04:00Z",
          },
        ],
      ],
      [
        "jra:05:03",
        [
          {
            horseNumber: "1",
            modelVersion: "model-v1",
            predictionGeneratedAt: "2026-08-15T00:04:00Z",
          },
        ],
      ],
      [
        "jra:05:04",
        [
          {
            horseNumber: "1",
            modelVersion: "model-v2",
            predictionGeneratedAt: "2026-08-15T00:06:00Z",
          },
        ],
      ],
    ]),
    now: NOW,
    predictions: [
      {
        generated_at: "2026-08-15T00:06:00Z",
        ketto_toroku_bango: "H01",
        keibajo_code: "05",
        model_version: "model-v2",
        race_bango: "01",
        source: "jra",
        umaban: 1,
      },
      {
        generated_at: "2026-08-15T00:04:00Z",
        ketto_toroku_bango: "H02",
        keibajo_code: "05",
        model_version: "model-v1",
        race_bango: "02",
        source: "jra",
        umaban: 1,
      },
      {
        generated_at: "2026-08-15T00:06:00Z",
        ketto_toroku_bango: "H03",
        keibajo_code: "05",
        model_version: "model-v2",
        race_bango: "03",
        source: "jra",
        umaban: 1,
      },
      {
        generated_at: "2026-08-15T00:06:00Z",
        ketto_toroku_bango: "H04",
        keibajo_code: "05",
        model_version: "model-v2",
        race_bango: "04",
        source: "jra",
        umaban: 1,
      },
      {
        generated_at: "2026-08-15T00:06:00Z",
        ketto_toroku_bango: "0000000000",
        keibajo_code: "05",
        race_bango: "05",
        source: "jra",
        umaban: 0,
      },
    ],
    races: [
      {
        keibajo_code: "05",
        last_weight_fetch_at: "2026-08-15T00:05:00Z",
        race_bango: "01",
        race_start_at_jst: "2026-08-15T10:00:00+09:00",
        source: "jra",
        weight_snapshot_at: null,
        weight_snapshot_count: 0,
      },
      {
        keibajo_code: "05",
        last_weight_fetch_at: "2026-08-15T00:05:00Z",
        race_bango: "02",
        race_start_at_jst: "2026-08-15T10:10:00+09:00",
        source: "jra",
        weight_snapshot_at: "2026-08-15T00:05:00Z",
        weight_snapshot_count: 1,
      },
      {
        keibajo_code: "05",
        last_weight_fetch_at: "2026-08-15T00:05:00Z",
        race_bango: "03",
        race_start_at_jst: "2026-08-15T10:20:00+09:00",
        source: "jra",
        weight_snapshot_at: "2026-08-15T00:05:00Z",
        weight_snapshot_count: 1,
      },
      {
        keibajo_code: "05",
        last_weight_fetch_at: "2026-08-15T00:05:00Z",
        race_bango: "04",
        race_start_at_jst: "2026-08-15T10:30:00+09:00",
        source: "jra",
        weight_snapshot_at: "2026-08-15T00:05:00Z",
        weight_snapshot_count: "invalid",
      },
    ],
    runYmd: "20260815",
  });

  expect(result.races[0]?.postWeight.reason).toBe("weight-snapshot-missing");
  expect(result.races[1]?.postWeight).toMatchObject({
    missingCount: 1,
    predictionAfterWeightCount: 0,
    reason: "prediction-before-weight",
  });
  expect(result.races[2]?.postWeight.reason).toBe("kv-generation-mismatch");
  expect(result.races[3]?.postWeight).toMatchObject({
    reason: "weight-snapshot-incomplete",
    weightSnapshotCount: 0,
  });
});

it("uses strict Catalog entries when D1 has no race entries", () => {
  const result = buildPredictionReadiness({
    catalogEntries: [
      {
        keibajo_code: "35",
        ketto_toroku_bango: "H1",
        race_bango: "01",
        source: "nar",
        umaban: 1,
      },
      {
        keibajo_code: "35",
        ketto_toroku_bango: "H2",
        race_bango: "01",
        source: "nar",
        umaban: 2,
      },
    ],
    entries: [],
    kvPayloads: new Map([
      [
        "nar:35:01",
        [
          {
            horseNumber: "1",
            modelVersion: "model-v1",
            predictionGeneratedAt: "2026-08-14T13:00:00Z",
          },
          {
            horseNumber: "2",
            modelVersion: "model-v1",
            predictionGeneratedAt: "2026-08-14T13:00:00Z",
          },
        ],
      ],
    ]),
    now: NOW,
    predictions: [
      {
        generated_at: "2026-08-14T13:00:00Z",
        ketto_toroku_bango: "H1",
        keibajo_code: "35",
        model_version: "model-v1",
        race_bango: "01",
        source: "nar",
        umaban: 1,
      },
      {
        generated_at: "2026-08-14T13:00:00Z",
        ketto_toroku_bango: "H2",
        keibajo_code: "35",
        model_version: "model-v1",
        race_bango: "01",
        source: "nar",
        umaban: 2,
      },
    ],
    races: [
      {
        keibajo_code: "35",
        race_bango: "01",
        race_start_at_jst: "2026-08-15T10:00:00+09:00",
        source: "nar",
      },
    ],
    runYmd: "20260815",
  });

  expect(result.races[0]).toMatchObject({
    expectedCount: 2,
    expectedSource: "catalog",
    preWeight: { complete: true, kvPredictionCount: 2, predictionCount: 2 },
  });
});

it("keeps D1 authoritative and rejects extra Neon or KV horses for Catalog fallback", () => {
  const result = buildPredictionReadiness({
    catalogEntries: [
      {
        keibajo_code: "35",
        ketto_toroku_bango: "H1",
        race_bango: "01",
        source: "nar",
        umaban: 1,
      },
      {
        keibajo_code: "35",
        ketto_toroku_bango: "H2",
        race_bango: "01",
        source: "nar",
        umaban: 2,
      },
      {
        keibajo_code: "35",
        ketto_toroku_bango: "H3",
        race_bango: "02",
        source: "nar",
        umaban: 1,
      },
    ],
    entries: [
      {
        keibajo_code: "35",
        ketto_toroku_bango: "H1",
        race_bango: "01",
        source: "nar",
        umaban: 1,
      },
    ],
    kvPayloads: new Map([
      [
        "nar:35:02",
        [
          {
            horseNumber: "1",
            modelVersion: "model-v1",
            predictionGeneratedAt: "2026-08-15T00:00:00Z",
          },
          {
            horseNumber: "2",
            modelVersion: "model-v1",
            predictionGeneratedAt: "2026-08-15T00:00:00Z",
          },
        ],
      ],
    ]),
    now: NOW,
    predictions: [
      {
        generated_at: "2026-08-15T00:00:00Z",
        ketto_toroku_bango: "H1",
        keibajo_code: "35",
        race_bango: "01",
        source: "nar",
        umaban: 1,
      },
      {
        generated_at: "2026-08-15T00:00:00Z",
        ketto_toroku_bango: "H3",
        keibajo_code: "35",
        model_version: "model-v1",
        race_bango: "02",
        source: "nar",
        umaban: 1,
      },
      {
        generated_at: "2026-08-15T00:00:00Z",
        ketto_toroku_bango: "H4",
        keibajo_code: "35",
        model_version: "model-v1",
        race_bango: "02",
        source: "nar",
        umaban: 2,
      },
    ],
    races: [
      {
        keibajo_code: "35",
        race_bango: "01",
        race_start_at_jst: "2026-08-15T10:00:00+09:00",
        source: "nar",
      },
      {
        keibajo_code: "35",
        race_bango: "02",
        race_start_at_jst: "2026-08-15T10:10:00+09:00",
        source: "nar",
      },
    ],
    runYmd: "20260815",
  });

  expect(result.races[0]).toMatchObject({ expectedCount: 1, expectedSource: "d1" });
  expect(result.races[1]).toMatchObject({
    expectedCount: 1,
    expectedSource: "catalog",
    preWeight: { complete: false, neonComplete: false },
  });
});

it("rejects an extra KV horse even when Catalog and Neon horse sets match", () => {
  const result = buildPredictionReadiness({
    catalogEntries: [
      {
        keibajo_code: "35",
        ketto_toroku_bango: "H1",
        race_bango: "01",
        source: "nar",
        umaban: 1,
      },
    ],
    entries: [],
    kvPayloads: new Map([
      [
        "nar:35:01",
        [
          {
            horseNumber: "1",
            modelVersion: "model-v1",
            predictionGeneratedAt: "2026-08-15T00:00:00Z",
          },
          {
            horseNumber: "2",
            modelVersion: "model-v1",
            predictionGeneratedAt: "2026-08-15T00:00:00Z",
          },
        ],
      ],
    ]),
    now: NOW,
    predictions: [
      {
        generated_at: "2026-08-15T00:00:00Z",
        ketto_toroku_bango: "H1",
        keibajo_code: "35",
        model_version: "model-v1",
        race_bango: "01",
        source: "nar",
        umaban: 1,
      },
    ],
    races: [
      {
        keibajo_code: "35",
        race_bango: "01",
        race_start_at_jst: "2026-08-15T10:00:00+09:00",
        source: "nar",
      },
    ],
    runYmd: "20260815",
  });

  expect(result.races[0]?.preWeight).toMatchObject({
    complete: false,
    kvComplete: false,
    neonComplete: true,
    reason: "kv-generation-mismatch",
  });
});

it("loads missing NAR entries from the authenticated Catalog bulk endpoint", async () => {
  const allMock = vi
    .fn()
    .mockResolvedValueOnce({
      results: [
        {
          keibajo_code: "35",
          race_bango: "01",
          race_start_at_jst: "2026-08-15T10:00:00+09:00",
          source: "nar",
        },
      ],
    })
    .mockResolvedValueOnce({ results: [] })
    .mockResolvedValueOnce({ results: [] });
  const catalogFetchMock = vi.fn(async (_request: Request) =>
    Response.json({
      date: "20260815",
      entries: [
        {
          keibajoCode: "35",
          kettoTorokuBango: "H1",
          raceBango: "01",
          source: "nar",
          umaban: 1,
        },
      ],
      source: "nar",
    }),
  );
  const getMock = vi.fn(async () => [
    {
      horseNumber: "1",
      modelVersion: "model-v1",
      predictionGeneratedAt: "2026-08-14T13:00:00Z",
    },
  ]);
  queryMock.mockResolvedValue([
    {
      generated_at: "2026-08-14T13:00:00Z",
      ketto_toroku_bango: "H1",
      keibajo_code: "35",
      model_version: "model-v1",
      race_bango: "01",
      source: "nar",
      umaban: 1,
    },
  ]);
  const env = {
    DETAIL_SECTION_CACHE_KV: { get: getMock },
    FINISH_POSITION_ATTESTATION_TOKEN: "attestation-secret",
    NEON_DATABASE_URL: "postgres://example",
    PC_KEIBA_R2_CATALOG: { fetch: catalogFetchMock },
    REALTIME_DB: {
      prepare: vi.fn(() => ({ bind: vi.fn(() => ({ all: allMock })) })),
    },
  } as unknown as Env;

  const result = await getPredictionReadiness({ env, now: NOW, runYmd: "20260815" });

  expect(catalogFetchMock).toHaveBeenCalledTimes(1);
  const request = catalogFetchMock.mock.calls[0]?.[0];
  expect(request?.url).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/internal/fresh-race-entries-bulk?date=20260815&source=nar",
  );
  expect(request?.headers.get("Authorization")).toBe("Bearer attestation-secret");
  expect(result.races[0]).toMatchObject({
    expectedCount: 1,
    expectedSource: "catalog",
    preWeight: { complete: true },
  });
});

it("fails closed when missing D1 entries cannot be loaded from Catalog", async () => {
  const allMock = vi
    .fn()
    .mockResolvedValueOnce({
      results: [
        {
          keibajo_code: "83",
          race_bango: "01",
          race_start_at_jst: "2026-08-15T10:00:00+09:00",
          source: "nar",
        },
      ],
    })
    .mockResolvedValueOnce({ results: [] })
    .mockResolvedValueOnce({ results: [] });
  queryMock.mockResolvedValue([]);
  const env = {
    NEON_DATABASE_URL: "postgres://example",
    REALTIME_DB: {
      prepare: vi.fn(() => ({ bind: vi.fn(() => ({ all: allMock })) })),
    },
  } as unknown as Env;

  await expect(getPredictionReadiness({ env, now: NOW, runYmd: "20260815" })).rejects.toThrow(
    "Catalog binding and attestation token are required for readiness entries",
  );
});

it("uses D1 for NAR and calls Catalog only for a missing ban-ei race", async () => {
  const allMock = vi
    .fn()
    .mockResolvedValueOnce({
      results: [
        {
          keibajo_code: "35",
          race_bango: "01",
          race_start_at_jst: "2026-08-15T10:00:00+09:00",
          source: "nar",
        },
        {
          keibajo_code: "83",
          race_bango: "01",
          race_start_at_jst: "2026-08-15T10:10:00+09:00",
          source: "nar",
        },
      ],
    })
    .mockResolvedValueOnce({
      results: [
        {
          ketto_toroku_bango: "N1",
          keibajo_code: "35",
          race_bango: "01",
          source: "nar",
          umaban: "1",
        },
      ],
    })
    .mockResolvedValueOnce({ results: [] });
  const catalogFetchMock = vi.fn(async (_request: Request) =>
    Response.json({
      date: "20260815",
      entries: [
        {
          keibajoCode: "83",
          kettoTorokuBango: "B1",
          raceBango: "01",
          source: "ban-ei",
          umaban: 1,
        },
      ],
      source: "ban-ei",
    }),
  );
  queryMock.mockResolvedValue([
    {
      generated_at: "2026-08-14T13:00:00Z",
      ketto_toroku_bango: "N1",
      keibajo_code: "35",
      race_bango: "01",
      source: "nar",
      umaban: 1,
    },
    {
      generated_at: "2026-08-14T13:00:00Z",
      ketto_toroku_bango: "B1",
      keibajo_code: "83",
      race_bango: "01",
      source: "nar",
      umaban: 1,
    },
  ]);
  const env = {
    FINISH_POSITION_ATTESTATION_TOKEN: "attestation-secret",
    NEON_DATABASE_URL: "postgres://example",
    PC_KEIBA_R2_CATALOG: { fetch: catalogFetchMock },
    REALTIME_DB: {
      prepare: vi.fn(() => ({ bind: vi.fn(() => ({ all: allMock })) })),
    },
  } as unknown as Env;

  const result = await getPredictionReadiness({ env, now: NOW, runYmd: "20260815" });

  expect(catalogFetchMock).toHaveBeenCalledTimes(1);
  expect(catalogFetchMock.mock.calls[0]?.[0].url).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/internal/fresh-race-entries-bulk?date=20260815&source=ban-ei",
  );
  expect(result.races[0]).toMatchObject({ expectedCount: 1, expectedSource: "d1" });
  expect(result.races[1]).toMatchObject({ expectedCount: 1, expectedSource: "catalog" });
});

it("fails closed on a non-success Catalog bulk response", async () => {
  const allMock = vi
    .fn()
    .mockResolvedValueOnce({
      results: [
        {
          keibajo_code: "35",
          race_bango: "01",
          race_start_at_jst: "2026-08-15T10:00:00+09:00",
          source: "nar",
        },
      ],
    })
    .mockResolvedValueOnce({ results: [] })
    .mockResolvedValueOnce({ results: [] });
  queryMock.mockResolvedValue([]);
  const env = {
    FINISH_POSITION_ATTESTATION_TOKEN: "attestation-secret",
    NEON_DATABASE_URL: "postgres://example",
    PC_KEIBA_R2_CATALOG: { fetch: vi.fn(async () => new Response(null, { status: 502 })) },
    REALTIME_DB: {
      prepare: vi.fn(() => ({ bind: vi.fn(() => ({ all: allMock })) })),
    },
  } as unknown as Env;

  await expect(getPredictionReadiness({ env, now: NOW, runYmd: "20260815" })).rejects.toThrow(
    "Catalog bulk readiness entries failed with HTTP 502",
  );
});

it("fails closed on an invalid Catalog bulk envelope", async () => {
  const allMock = vi
    .fn()
    .mockResolvedValueOnce({
      results: [
        {
          keibajo_code: "35",
          race_bango: "01",
          race_start_at_jst: "2026-08-15T10:00:00+09:00",
          source: "nar",
        },
      ],
    })
    .mockResolvedValueOnce({ results: [] })
    .mockResolvedValueOnce({ results: [] });
  queryMock.mockResolvedValue([]);
  const env = {
    FINISH_POSITION_ATTESTATION_TOKEN: "attestation-secret",
    NEON_DATABASE_URL: "postgres://example",
    PC_KEIBA_R2_CATALOG: {
      fetch: vi.fn(async () => Response.json({ date: "20260814", entries: [], source: "nar" })),
    },
    REALTIME_DB: {
      prepare: vi.fn(() => ({ bind: vi.fn(() => ({ all: allMock })) })),
    },
  } as unknown as Env;

  await expect(getPredictionReadiness({ env, now: NOW, runYmd: "20260815" })).rejects.toThrow(
    "Catalog bulk readiness entries returned an invalid envelope",
  );
});

it("rejects a ban-ei bulk entry outside venue 83", async () => {
  const allMock = vi
    .fn()
    .mockResolvedValueOnce({
      results: [
        {
          keibajo_code: "83",
          race_bango: "01",
          race_start_at_jst: "2026-08-15T10:00:00+09:00",
          source: "nar",
        },
      ],
    })
    .mockResolvedValueOnce({ results: [] })
    .mockResolvedValueOnce({ results: [] });
  queryMock.mockResolvedValue([]);
  const env = {
    FINISH_POSITION_ATTESTATION_TOKEN: "attestation-secret",
    NEON_DATABASE_URL: "postgres://example",
    PC_KEIBA_R2_CATALOG: {
      fetch: vi.fn(async () =>
        Response.json({
          date: "20260815",
          entries: [
            {
              keibajoCode: "35",
              kettoTorokuBango: "H1",
              raceBango: "01",
              source: "ban-ei",
              umaban: 1,
            },
          ],
          source: "ban-ei",
        }),
      ),
    },
    REALTIME_DB: {
      prepare: vi.fn(() => ({ bind: vi.fn(() => ({ all: allMock })) })),
    },
  } as unknown as Env;

  await expect(getPredictionReadiness({ env, now: NOW, runYmd: "20260815" })).rejects.toThrow(
    "Catalog bulk readiness entries returned an invalid entry",
  );
});

it("fails closed when Catalog omits a missing race", async () => {
  const allMock = vi
    .fn()
    .mockResolvedValueOnce({
      results: [
        {
          keibajo_code: "35",
          race_bango: "01",
          race_start_at_jst: "2026-08-15T10:00:00+09:00",
          source: "nar",
        },
      ],
    })
    .mockResolvedValueOnce({ results: [] })
    .mockResolvedValueOnce({ results: [] });
  queryMock.mockResolvedValue([]);
  const env = {
    FINISH_POSITION_ATTESTATION_TOKEN: "attestation-secret",
    NEON_DATABASE_URL: "postgres://example",
    PC_KEIBA_R2_CATALOG: {
      fetch: vi.fn(async () =>
        Response.json({
          date: "20260815",
          entries: [
            {
              keibajoCode: "35",
              kettoTorokuBango: "H2",
              raceBango: "02",
              source: "nar",
              umaban: 2,
            },
          ],
          source: "nar",
        }),
      ),
    },
    REALTIME_DB: {
      prepare: vi.fn(() => ({ bind: vi.fn(() => ({ all: allMock })) })),
    },
  } as unknown as Env;

  await expect(getPredictionReadiness({ env, now: NOW, runYmd: "20260815" })).rejects.toThrow(
    "Catalog bulk readiness entries did not cover race categories: nar",
  );
});
