// Run with bun. `bun run --filter pc-keiba-viewer test`
import { afterEach, beforeEach, expect, it } from "vitest";

import {
  appendSnapshot,
  buildHistoryByType,
  buildHorseTrends,
  buildTanshoHistoryPoints,
  buildTrendsByType,
  readHistory,
  resetHistoryStore,
} from "./dev-realtime-history-store.server";

beforeEach(() => {
  resetHistoryStore();
});

afterEach(() => {
  resetHistoryStore();
});

it("appendSnapshot then readHistory returns the appended snapshot", () => {
  appendSnapshot("nar:2026:0529:47:01", {
    byType: {
      tansho: [{ combination: "1", odds: 2.5, rank: 1 }],
    },
    fetchedAt: "2026-05-29T07:30:00.000Z",
  });
  expect(readHistory("nar:2026:0529:47:01")).toStrictEqual([
    {
      byType: {
        tansho: [{ combination: "1", odds: 2.5, rank: 1 }],
      },
      fetchedAt: "2026-05-29T07:30:00.000Z",
    },
  ]);
});

it("appendSnapshot dedupes a repeated fetchedAt at the tail", () => {
  appendSnapshot("nar:2026:0529:47:01", {
    byType: {
      tansho: [{ combination: "1", odds: 2.5, rank: 1 }],
    },
    fetchedAt: "2026-05-29T07:30:00.000Z",
  });
  appendSnapshot("nar:2026:0529:47:01", {
    byType: {
      tansho: [{ combination: "1", odds: 9.9, rank: 5 }],
    },
    fetchedAt: "2026-05-29T07:30:00.000Z",
  });
  expect(readHistory("nar:2026:0529:47:01").length).toBe(1);
});

it("appendSnapshot drops the oldest snapshot once the window exceeds 60 points", () => {
  const fillerSnapshots = Array.from({ length: 61 }, (_value, index) => ({
    byType: { tansho: [{ combination: "1", odds: index + 1 }] },
    fetchedAt: `2026-05-29T07:00:${String(index).padStart(2, "0")}.000Z`,
  }));
  fillerSnapshots.forEach((snap) => {
    appendSnapshot("nar:2026:0529:47:01", snap);
  });
  const stored = readHistory("nar:2026:0529:47:01");
  expect(stored.length).toBe(60);
  expect(stored[0]?.fetchedAt).toBe("2026-05-29T07:00:01.000Z");
  expect(stored.at(-1)?.fetchedAt).toBe("2026-05-29T07:00:60.000Z");
});

it("appendSnapshot keeps independent raceKey histories", () => {
  appendSnapshot("nar:2026:0529:47:01", {
    byType: { tansho: [{ combination: "1", odds: 1.5, rank: 1 }] },
    fetchedAt: "2026-05-29T07:30:00.000Z",
  });
  appendSnapshot("nar:2026:0529:47:02", {
    byType: { tansho: [{ combination: "2", odds: 4.2, rank: 1 }] },
    fetchedAt: "2026-05-29T07:30:00.000Z",
  });
  expect(readHistory("nar:2026:0529:47:01").length).toBe(1);
  expect(readHistory("nar:2026:0529:47:02").length).toBe(1);
});

it("readHistory returns an empty array for an unknown raceKey", () => {
  expect(readHistory("nar:9999:0101:99:99")).toStrictEqual([]);
});

it("resetHistoryStore clears all races", () => {
  appendSnapshot("nar:2026:0529:47:01", {
    byType: { tansho: [{ combination: "1", odds: 1.5, rank: 1 }] },
    fetchedAt: "2026-05-29T07:30:00.000Z",
  });
  resetHistoryStore();
  expect(readHistory("nar:2026:0529:47:01")).toStrictEqual([]);
});

it("buildTanshoHistoryPoints flattens tansho per snapshot using combination as horseNumber", () => {
  const points = buildTanshoHistoryPoints([
    {
      byType: {
        tansho: [
          { combination: "1", odds: 1.5, rank: 1 },
          { combination: "2", odds: 3, rank: 2 },
        ],
      },
      fetchedAt: "2026-05-29T07:30:00.000Z",
    },
    {
      byType: {
        tansho: [
          { combination: "1", odds: 1.7, rank: 1 },
          { combination: "2", odds: 2.9, rank: 2 },
        ],
      },
      fetchedAt: "2026-05-29T07:30:30.000Z",
    },
  ]);
  expect(points).toStrictEqual([
    {
      fetchedAt: "2026-05-29T07:30:00.000Z",
      horseNumber: "1",
      odds: 1.5,
      popularity: 1,
    },
    {
      fetchedAt: "2026-05-29T07:30:00.000Z",
      horseNumber: "2",
      odds: 3,
      popularity: 2,
    },
    {
      fetchedAt: "2026-05-29T07:30:30.000Z",
      horseNumber: "1",
      odds: 1.7,
      popularity: 1,
    },
    {
      fetchedAt: "2026-05-29T07:30:30.000Z",
      horseNumber: "2",
      odds: 2.9,
      popularity: 2,
    },
  ]);
});

it("buildTanshoHistoryPoints returns empty for snapshots without tansho", () => {
  expect(
    buildTanshoHistoryPoints([
      {
        byType: { fukusho: [{ combination: "1", averageOdds: 2 }] },
        fetchedAt: "2026-05-29T07:30:00.000Z",
      },
    ]),
  ).toStrictEqual([]);
});

it("buildTanshoHistoryPoints falls back to averageOdds when odds is absent and nulls when neither is present", () => {
  const points = buildTanshoHistoryPoints([
    {
      byType: {
        tansho: [{ combination: "1", averageOdds: 2.2 }, { combination: "2" }],
      },
      fetchedAt: "2026-05-29T07:30:00.000Z",
    },
  ]);
  expect(points).toStrictEqual([
    {
      fetchedAt: "2026-05-29T07:30:00.000Z",
      horseNumber: "1",
      odds: 2.2,
      popularity: null,
    },
    {
      fetchedAt: "2026-05-29T07:30:00.000Z",
      horseNumber: "2",
      odds: null,
      popularity: null,
    },
  ]);
});

it("buildHorseTrends groups history points by horseNumber", () => {
  expect(
    buildHorseTrends([
      {
        fetchedAt: "2026-05-29T07:30:00.000Z",
        horseNumber: "1",
        odds: 1.5,
        popularity: 1,
      },
      {
        fetchedAt: "2026-05-29T07:30:30.000Z",
        horseNumber: "1",
        odds: 1.7,
        popularity: 1,
      },
      {
        fetchedAt: "2026-05-29T07:30:00.000Z",
        horseNumber: "2",
        odds: 3,
        popularity: 2,
      },
    ]),
  ).toStrictEqual([
    {
      horseNumber: "1",
      points: [
        {
          fetchedAt: "2026-05-29T07:30:00.000Z",
          horseNumber: "1",
          odds: 1.5,
          popularity: 1,
        },
        {
          fetchedAt: "2026-05-29T07:30:30.000Z",
          horseNumber: "1",
          odds: 1.7,
          popularity: 1,
        },
      ],
    },
    {
      horseNumber: "2",
      points: [
        {
          fetchedAt: "2026-05-29T07:30:00.000Z",
          horseNumber: "2",
          odds: 3,
          popularity: 2,
        },
      ],
    },
  ]);
});

it("buildHistoryByType emits tansho and fukusho entries while skipping empty types", () => {
  expect(
    buildHistoryByType([
      {
        byType: {
          fukusho: [{ combination: "1", averageOdds: 2, rank: 1 }],
          tansho: [{ combination: "1", odds: 1.5, rank: 1 }],
        },
        fetchedAt: "2026-05-29T07:30:00.000Z",
      },
    ]),
  ).toStrictEqual({
    fukusho: [
      {
        combination: "1",
        fetchedAt: "2026-05-29T07:30:00.000Z",
        odds: 2,
        rank: 1,
      },
    ],
    tansho: [
      {
        combination: "1",
        fetchedAt: "2026-05-29T07:30:00.000Z",
        odds: 1.5,
        rank: 1,
      },
    ],
  });
});

it("buildHistoryByType returns an empty record when no supported types are present", () => {
  expect(
    buildHistoryByType([
      {
        byType: {},
        fetchedAt: "2026-05-29T07:30:00.000Z",
      },
    ]),
  ).toStrictEqual({});
});

it("buildTrendsByType groups trend points by combination per odds type", () => {
  expect(
    buildTrendsByType({
      tansho: [
        {
          combination: "1",
          fetchedAt: "2026-05-29T07:30:00.000Z",
          odds: 1.5,
          rank: 1,
        },
        {
          combination: "1",
          fetchedAt: "2026-05-29T07:30:30.000Z",
          odds: 1.7,
          rank: 1,
        },
        {
          combination: "2",
          fetchedAt: "2026-05-29T07:30:00.000Z",
          odds: 3,
          rank: 2,
        },
      ],
    }),
  ).toStrictEqual({
    tansho: [
      {
        combination: "1",
        points: [
          {
            combination: "1",
            fetchedAt: "2026-05-29T07:30:00.000Z",
            odds: 1.5,
            rank: 1,
          },
          {
            combination: "1",
            fetchedAt: "2026-05-29T07:30:30.000Z",
            odds: 1.7,
            rank: 1,
          },
        ],
      },
      {
        combination: "2",
        points: [
          {
            combination: "2",
            fetchedAt: "2026-05-29T07:30:00.000Z",
            odds: 3,
            rank: 2,
          },
        ],
      },
    ],
  });
});
