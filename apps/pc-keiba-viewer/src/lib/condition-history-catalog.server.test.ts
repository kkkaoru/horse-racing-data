// Run with bun (bunx vitest).
import { beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const { safeGetCloudflareEnvMock } = vi.hoisted(() => ({
  safeGetCloudflareEnvMock: vi.fn<() => Promise<CloudflareEnv | null>>(),
}));

vi.mock("./cloudflare-context.server", () => ({
  safeGetCloudflareEnv: safeGetCloudflareEnvMock,
}));

import {
  buildConditionHistoryCatalogUrl,
  fetchConditionHistoryStatsFromCatalog,
} from "./condition-history-catalog.server";

const query = {
  day: "8",
  includeDistance: true,
  includeSurface: true,
  includeTurn: false,
  includeVenue: true,
  keibajoCode: "7",
  month: "8",
  raceNumber: "8",
  source: "jra" as const,
  year: "2026",
  years: 10,
};

const catalogPayload = {
  carriedWeightClassStats: [
    {
      key: "55.5-57",
      quinellaCount: 2,
      quinellaRate: 20,
      showCount: 3,
      showRate: 30,
      starts: 10,
      winCount: 1,
      winRate: 10,
    },
  ],
  finishPositionStats: [
    {
      averageOdds: 3.2,
      averagePopularity: 2.5,
      count: 4,
      details: [{ date: "20260101" }],
      finishPosition: 1,
      medianOdds: 3,
      medianPopularity: 2,
    },
  ],
  frameStats: [
    {
      averageFinish: 4.2,
      averagePopularity: 5.1,
      count: 8,
      details: [{ date: "20260101" }],
      frameNumber: "1",
      medianFinish: 4,
      medianPopularity: 5,
      quinellaCount: 2,
      quinellaRate: 25,
      runnerCount: 2,
      score: 12.5,
      showCount: 3,
      showRate: 37.5,
      winCount: 1,
      winRate: 12.5,
    },
  ],
  raceTimeStats: {
    averageKohan3f: 35.1,
    averageRaceTime: 96.4,
    correlationRows: [
      {
        details: [
          {
            key: "jockeyShow",
            label: "騎手",
            reason: "同条件",
            score: 0.7,
            target: 0.3,
            value: 0.4,
            weight: 0.2,
          },
        ],
        horseName: "イクイノックス",
        horseNumber: "1",
        score: 0.8,
      },
    ],
    fastestDetail: { date: "20260101" },
    fastestKohan3f: 33.8,
    fastestRaceTime: 94.2,
    medianKohan3f: 35,
    medianRaceTime: 96,
    raceCount: 12,
    targetRaces: [{ date: "20260101" }],
  },
  weightClassStats: [
    {
      key: "480-499",
      quinellaCount: 4,
      quinellaRate: 40,
      showCount: 5,
      showRate: 50,
      starts: 10,
      winCount: 2,
      winRate: 20,
    },
  ],
};

beforeEach(() => {
  safeGetCloudflareEnvMock.mockReset();
});

it("builds the condition-history Catalog URL with padded race identity and include flags", () => {
  expect(buildConditionHistoryCatalogUrl(query).toString()).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/condition-history-stats?year=2026&month=08&day=08&keibajoCode=07&raceNumber=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=0",
  );
});

it("maps Catalog condition history aggregates onto viewer stats with empty details", async () => {
  const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json(catalogPayload));
  safeGetCloudflareEnvMock.mockResolvedValue({ R2_CATALOG: { fetch: fetchMock } });

  await expect(fetchConditionHistoryStatsFromCatalog(query)).resolves.toStrictEqual({
    carriedWeightClassStats: [
      {
        key: "55.5-57",
        quinellaCount: 2,
        quinellaRate: 20,
        showCount: 3,
        showRate: 30,
        starts: 10,
        winCount: 1,
        winRate: 10,
      },
    ],
    finishPositionStats: [
      {
        averageOdds: 3.2,
        averagePopularity: 2.5,
        count: 4,
        details: [],
        finishPosition: 1,
        medianOdds: 3,
        medianPopularity: 2,
      },
    ],
    frameStats: [
      {
        averageFinish: 4.2,
        averagePopularity: 5.1,
        count: 8,
        details: [],
        frameNumber: "1",
        medianFinish: 4,
        medianPopularity: 5,
        quinellaCount: 2,
        quinellaRate: 25,
        runnerCount: 2,
        score: 12.5,
        showCount: 3,
        showRate: 37.5,
        winCount: 1,
        winRate: 12.5,
      },
    ],
    raceTimeStats: {
      averageKohan3f: 35.1,
      averageRaceTime: 96.4,
      correlationRows: [
        {
          details: [
            {
              key: "jockeyShow",
              label: "騎手",
              reason: "同条件",
              score: 0.7,
              target: 0.3,
              value: 0.4,
              weight: 0.2,
            },
          ],
          horseName: "イクイノックス",
          horseNumber: "1",
          score: 0.8,
        },
      ],
      fastestDetail: null,
      fastestKohan3f: 33.8,
      fastestRaceTime: 94.2,
      medianKohan3f: 35,
      medianRaceTime: 96,
      raceCount: 12,
      targetRaces: [],
    },
    weightClassStats: [
      {
        key: "480-499",
        quinellaCount: 4,
        quinellaRate: 40,
        showCount: 5,
        showRate: 50,
        starts: 10,
        winCount: 2,
        winRate: 20,
      },
    ],
  });
  const request = fetchMock.mock.calls[0]?.[0];
  expect(request).toBeInstanceOf(Request);
  if (!(request instanceof Request)) throw new Error("Catalog Request expected");
  expect(request.url).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/condition-history-stats?year=2026&month=08&day=08&keibajoCode=07&raceNumber=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=0",
  );
});

it("accepts numeric Catalog fields and omitted optional arrays", async () => {
  const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
    Response.json({
      carriedWeightClassStats: [
        {
          key: 55,
          quinellaCount: "2",
          quinellaRate: null,
          showCount: 3,
          showRate: null,
          starts: "10",
          winCount: 1,
          winRate: null,
        },
      ],
      finishPositionStats: [
        {
          averageOdds: null,
          averagePopularity: null,
          count: "4",
          finishPosition: "1",
          medianOdds: null,
          medianPopularity: null,
        },
      ],
      frameStats: [
        {
          averageFinish: null,
          averagePopularity: null,
          count: "8",
          frameNumber: 1,
          medianFinish: null,
          medianPopularity: null,
          quinellaCount: "2",
          quinellaRate: null,
          runnerCount: null,
          score: "12.5",
          showCount: 3,
          showRate: null,
          winCount: 1,
          winRate: null,
        },
      ],
      raceTimeStats: {
        averageKohan3f: null,
        averageRaceTime: null,
        fastestKohan3f: null,
        fastestRaceTime: null,
        medianKohan3f: null,
        medianRaceTime: null,
        raceCount: "0",
      },
      weightClassStats: [
        {
          key: "480-499",
          quinellaCount: 0,
          quinellaRate: 0,
          showCount: 0,
          showRate: 0,
          starts: 0,
          winCount: 0,
          winRate: 0,
        },
      ],
    }),
  );
  safeGetCloudflareEnvMock.mockResolvedValue({ R2_CATALOG: { fetch: fetchMock } });

  await expect(fetchConditionHistoryStatsFromCatalog(query)).resolves.toStrictEqual({
    carriedWeightClassStats: [
      {
        key: "55",
        quinellaCount: 2,
        quinellaRate: null,
        showCount: 3,
        showRate: null,
        starts: 10,
        winCount: 1,
        winRate: null,
      },
    ],
    finishPositionStats: [
      {
        averageOdds: null,
        averagePopularity: null,
        count: 4,
        details: [],
        finishPosition: 1,
        medianOdds: null,
        medianPopularity: null,
      },
    ],
    frameStats: [
      {
        averageFinish: null,
        averagePopularity: null,
        count: 8,
        details: [],
        frameNumber: "1",
        medianFinish: null,
        medianPopularity: null,
        quinellaCount: 2,
        quinellaRate: null,
        runnerCount: null,
        score: 12.5,
        showCount: 3,
        showRate: null,
        winCount: 1,
        winRate: null,
      },
    ],
    raceTimeStats: {
      averageKohan3f: null,
      averageRaceTime: null,
      correlationRows: [],
      fastestDetail: null,
      fastestKohan3f: null,
      fastestRaceTime: null,
      medianKohan3f: null,
      medianRaceTime: null,
      raceCount: 0,
      targetRaces: [],
    },
    weightClassStats: [
      {
        key: "480-499",
        quinellaCount: 0,
        quinellaRate: 0,
        showCount: 0,
        showRate: 0,
        starts: 0,
        winCount: 0,
        winRate: 0,
      },
    ],
  });
});

it("returns null when the Catalog binding is unavailable", async () => {
  safeGetCloudflareEnvMock.mockResolvedValue(null);
  await expect(fetchConditionHistoryStatsFromCatalog(query)).resolves.toBeNull();
});

it("throws when Catalog HTTP, payload, or rows are invalid", async () => {
  const fetchMock = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(new Response("unavailable", { status: 502 }))
    .mockResolvedValueOnce(Response.json("invalid"))
    .mockResolvedValueOnce(
      Response.json({
        ...catalogPayload,
        frameStats: [{ ...catalogPayload.frameStats[0], count: "bad" }],
      }),
    )
    .mockResolvedValueOnce(
      Response.json({
        ...catalogPayload,
        frameStats: [{ ...catalogPayload.frameStats[0], details: "bad" }],
      }),
    )
    .mockResolvedValueOnce(
      Response.json({
        ...catalogPayload,
        weightClassStats: [{ ...catalogPayload.weightClassStats[0], key: "" }],
      }),
    )
    .mockResolvedValueOnce(
      Response.json({
        ...catalogPayload,
        finishPositionStats: [{ ...catalogPayload.finishPositionStats[0], details: "bad" }],
      }),
    )
    .mockResolvedValueOnce(
      Response.json({
        ...catalogPayload,
        raceTimeStats: { ...catalogPayload.raceTimeStats, correlationRows: "bad" },
      }),
    )
    .mockResolvedValueOnce(
      Response.json({
        ...catalogPayload,
        raceTimeStats: {
          ...catalogPayload.raceTimeStats,
          correlationRows: [
            {
              details: [
                {
                  key: "unknown",
                  label: "A",
                  reason: "B",
                  score: 1,
                  target: 1,
                  value: 1,
                  weight: 1,
                },
              ],
              horseName: "A",
              horseNumber: "1",
              score: 1,
            },
          ],
        },
      }),
    )
    .mockResolvedValueOnce(
      Response.json({
        ...catalogPayload,
        raceTimeStats: {
          ...catalogPayload.raceTimeStats,
          correlationRows: [{ details: "bad", horseName: "A", horseNumber: "1", score: 1 }],
        },
      }),
    )
    .mockResolvedValueOnce(
      Response.json({
        ...catalogPayload,
        raceTimeStats: { ...catalogPayload.raceTimeStats, targetRaces: "bad" },
      }),
    )
    .mockResolvedValueOnce(
      Response.json({
        ...catalogPayload,
        raceTimeStats: { ...catalogPayload.raceTimeStats, raceCount: "bad" },
      }),
    );
  safeGetCloudflareEnvMock.mockResolvedValue({ R2_CATALOG: { fetch: fetchMock } });

  await expect(fetchConditionHistoryStatsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog condition history stats failed: 502",
  );
  await expect(fetchConditionHistoryStatsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog condition history stats payload is malformed",
  );
  await expect(fetchConditionHistoryStatsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog condition history stats rows are malformed",
  );
  await expect(fetchConditionHistoryStatsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog condition history stats rows are malformed",
  );
  await expect(fetchConditionHistoryStatsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog condition history stats rows are malformed",
  );
  await expect(fetchConditionHistoryStatsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog condition history stats rows are malformed",
  );
  await expect(fetchConditionHistoryStatsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog condition history stats rows are malformed",
  );
  await expect(fetchConditionHistoryStatsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog condition history stats rows are malformed",
  );
  await expect(fetchConditionHistoryStatsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog condition history stats rows are malformed",
  );
  await expect(fetchConditionHistoryStatsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog condition history stats rows are malformed",
  );
  await expect(fetchConditionHistoryStatsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog condition history stats rows are malformed",
  );
});

it("propagates Catalog fetch failures", async () => {
  const fetchMock = vi.fn<typeof fetch>().mockRejectedValue(new Error("catalog timeout"));
  safeGetCloudflareEnvMock.mockResolvedValue({ R2_CATALOG: { fetch: fetchMock } });
  await expect(fetchConditionHistoryStatsFromCatalog(query)).rejects.toThrow("catalog timeout");
});
