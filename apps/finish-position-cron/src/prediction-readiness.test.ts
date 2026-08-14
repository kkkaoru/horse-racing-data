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
    });
  const bindMock = vi.fn(() => ({ all: allMock }));
  const prepareMock = vi.fn(() => ({ bind: bindMock }));
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
  expect(prepareMock).toHaveBeenCalledTimes(2);
  expect(queryMock).toHaveBeenCalledTimes(1);
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
