import { beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const { safeGetCloudflareEnvMock } = vi.hoisted(() => ({
  safeGetCloudflareEnvMock: vi.fn<() => Promise<CloudflareEnv | null>>(),
}));

vi.mock("./cloudflare-context.server", () => ({
  safeGetCloudflareEnv: safeGetCloudflareEnvMock,
}));

import {
  buildWinRateHeatmapCatalogUrl,
  fetchWinRateHeatmapStatsFromCatalog,
  groupCatalogBloodlineRows,
  groupCatalogSimilarRows,
} from "./win-rate-heatmap-catalog.server";

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
  bloodlineRows: [
    {
      category: "sire",
      details: [],
      name: "ディープインパクト",
      places: 4,
      shows: 6,
      starts: 20,
      umaban: 1,
      wins: 2,
    },
  ],
  similarRows: [
    {
      details: [],
      kind: "jockey",
      name: "武豊",
      places: 3,
      shows: 5,
      starts: 10,
      umaban: 1,
      wins: 1,
    },
  ],
};

beforeEach(() => {
  safeGetCloudflareEnvMock.mockReset();
});

it("builds the heatmap Catalog URL with padded race identity and include flags", () => {
  expect(buildWinRateHeatmapCatalogUrl(query).toString()).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/win-rate-heatmap-stats?year=2026&month=08&day=08&keibajoCode=07&raceNumber=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=0",
  );
});

it("appends includeOwner=1 only when similar-section owner rows are requested", () => {
  expect(buildWinRateHeatmapCatalogUrl({ ...query, includeOwner: true }).toString()).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/win-rate-heatmap-stats?year=2026&month=08&day=08&keibajoCode=07&raceNumber=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=0&includeOwner=1",
  );
  expect(buildWinRateHeatmapCatalogUrl({ ...query, includeOwner: false }).toString()).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/win-rate-heatmap-stats?year=2026&month=08&day=08&keibajoCode=07&raceNumber=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=0",
  );
});

it("appends includeGrade=1 and includeTrackCode=1 only when those cell filters are requested", () => {
  expect(
    buildWinRateHeatmapCatalogUrl({
      ...query,
      includeGrade: true,
      includeTrackCode: true,
    }).toString(),
  ).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/win-rate-heatmap-stats?year=2026&month=08&day=08&keibajoCode=07&raceNumber=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=0&includeGrade=1&includeTrackCode=1",
  );
  expect(buildWinRateHeatmapCatalogUrl({ ...query, includeGrade: false }).toString()).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/win-rate-heatmap-stats?year=2026&month=08&day=08&keibajoCode=07&raceNumber=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=0",
  );
});

it("appends class, age, condition-key, and race-title flags only when requested", () => {
  expect(
    buildWinRateHeatmapCatalogUrl({
      ...query,
      includeAge: true,
      includeClass: true,
      includeConditionKey: true,
      includeRaceTitle: true,
    }).toString(),
  ).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/win-rate-heatmap-stats?year=2026&month=08&day=08&keibajoCode=07&raceNumber=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=0&includeAge=1&includeClass=1&includeConditionKey=1&includeRaceTitle=1",
  );
  expect(
    buildWinRateHeatmapCatalogUrl({
      ...query,
      includeAge: false,
      includeClass: false,
      includeConditionKey: false,
      includeRaceTitle: false,
    }).toString(),
  ).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/win-rate-heatmap-stats?year=2026&month=08&day=08&keibajoCode=07&raceNumber=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=0",
  );
});

it("appends includeJockeyFrame=1 only when heatmap jockey-frame rows are requested", () => {
  expect(buildWinRateHeatmapCatalogUrl({ ...query, includeJockeyFrame: true }).toString()).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/win-rate-heatmap-stats?year=2026&month=08&day=08&keibajoCode=07&raceNumber=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=0&includeJockeyFrame=1",
  );
  expect(buildWinRateHeatmapCatalogUrl({ ...query, includeOwner: true }).toString()).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/win-rate-heatmap-stats?year=2026&month=08&day=08&keibajoCode=07&raceNumber=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=0&includeOwner=1",
  );
  expect(buildWinRateHeatmapCatalogUrl({ ...query, includeJockeyFrame: false }).toString()).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/win-rate-heatmap-stats?year=2026&month=08&day=08&keibajoCode=07&raceNumber=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=0",
  );
});

it("maps Catalog aggregate rows onto heatmap stats without details", async () => {
  const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json(catalogPayload));
  safeGetCloudflareEnvMock.mockResolvedValue({ R2_CATALOG: { fetch: fetchMock } });

  await expect(fetchWinRateHeatmapStatsFromCatalog(query)).resolves.toStrictEqual({
    bloodlineRows: [
      {
        category: "sire",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 0,
        name: "ディープインパクト",
        quinellaCount: 4,
        quinellaRate: 20,
        showCount: 6,
        showRate: 30,
        starts: 20,
        winCount: 2,
        winRate: 10,
      },
    ],
    similarRows: [
      {
        category: "jockey",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 0,
        name: "武豊",
        quinellaCount: 3,
        quinellaRate: 30,
        showCount: 5,
        showRate: 50,
        starts: 10,
        winCount: 1,
        winRate: 10,
      },
    ],
  });
  const request = fetchMock.mock.calls[0]?.[0];
  expect(request).toBeInstanceOf(Request);
  if (!(request instanceof Request)) throw new Error("Catalog Request expected");
  expect(request.url).toBe(buildWinRateHeatmapCatalogUrl(query).toString());
});

it("maps zero-start Catalog rows to zero rates", async () => {
  const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
    Response.json({
      bloodlineRows: [
        {
          category: "damDamSire",
          details: [],
          name: "サンデーサイレンス",
          places: 0,
          shows: 0,
          starts: 0,
          umaban: 12,
          wins: 0,
        },
      ],
      similarRows: [
        {
          details: [],
          kind: "trainer",
          name: "不明",
          places: 0,
          shows: 0,
          starts: 0,
          umaban: 12,
          wins: 0,
        },
      ],
    }),
  );
  safeGetCloudflareEnvMock.mockResolvedValue({ R2_CATALOG: { fetch: fetchMock } });

  await expect(fetchWinRateHeatmapStatsFromCatalog(query)).resolves.toStrictEqual({
    bloodlineRows: [
      {
        category: "damDamSire",
        currentHorseNumbers: "12",
        details: [],
        horseCount: 0,
        name: "サンデーサイレンス",
        quinellaCount: 0,
        quinellaRate: 0,
        showCount: 0,
        showRate: 0,
        starts: 0,
        winCount: 0,
        winRate: 0,
      },
    ],
    similarRows: [
      {
        category: "trainer",
        currentHorseNumbers: "12",
        details: [],
        horseCount: 0,
        name: "不明",
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

it("accepts numeric Catalog fields that stringify to identifiers", async () => {
  const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
    Response.json({
      bloodlineRows: [
        {
          category: "sireSire",
          details: [],
          name: 10,
          places: "1",
          shows: "1",
          starts: "5",
          umaban: "3",
          wins: "1",
        },
      ],
      similarRows: [
        {
          details: [],
          kind: "trainer",
          name: 7,
          places: "2",
          shows: "2",
          starts: "4",
          umaban: "3",
          wins: "0",
        },
      ],
    }),
  );
  safeGetCloudflareEnvMock.mockResolvedValue({ R2_CATALOG: { fetch: fetchMock } });

  await expect(fetchWinRateHeatmapStatsFromCatalog(query)).resolves.toStrictEqual({
    bloodlineRows: [
      {
        category: "sireSire",
        currentHorseNumbers: "3",
        details: [],
        horseCount: 0,
        name: "10",
        quinellaCount: 1,
        quinellaRate: 20,
        showCount: 1,
        showRate: 20,
        starts: 5,
        winCount: 1,
        winRate: 20,
      },
    ],
    similarRows: [
      {
        category: "trainer",
        currentHorseNumbers: "3",
        details: [],
        horseCount: 0,
        name: "7",
        quinellaCount: 2,
        quinellaRate: 50,
        showCount: 2,
        showRate: 50,
        starts: 4,
        winCount: 0,
        winRate: 0,
      },
    ],
  });
});

it("maps jockeyFrame similar rows when includeJockeyFrame is enabled", async () => {
  const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
    Response.json({
      bloodlineRows: [],
      similarRows: [
        {
          details: [],
          kind: "jockeyFrame",
          name: "武豊",
          places: 4,
          shows: 5,
          starts: 10,
          umaban: 3,
          wins: 2,
        },
      ],
    }),
  );
  safeGetCloudflareEnvMock.mockResolvedValue({ R2_CATALOG: { fetch: fetchMock } });

  await expect(
    fetchWinRateHeatmapStatsFromCatalog({ ...query, includeJockeyFrame: true }),
  ).resolves.toStrictEqual({
    bloodlineRows: [],
    similarRows: [
      {
        category: "jockeyFrame",
        currentHorseNumbers: "3",
        details: [],
        horseCount: 0,
        name: "武豊",
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
    "https://pc-keiba-r2-catalog.internal/v1/win-rate-heatmap-stats?year=2026&month=08&day=08&keibajoCode=07&raceNumber=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=0&includeJockeyFrame=1",
  );
});

it("maps owner similar rows when includeOwner is enabled", async () => {
  const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
    Response.json({
      bloodlineRows: [],
      similarRows: [
        {
          details: [],
          kind: "owner",
          name: "社台レースホース",
          places: 2,
          shows: 3,
          starts: 8,
          umaban: 4,
          wins: 1,
        },
      ],
    }),
  );
  safeGetCloudflareEnvMock.mockResolvedValue({ R2_CATALOG: { fetch: fetchMock } });

  await expect(
    fetchWinRateHeatmapStatsFromCatalog({ ...query, includeOwner: true }),
  ).resolves.toStrictEqual({
    bloodlineRows: [],
    similarRows: [
      {
        category: "owner",
        currentHorseNumbers: "4",
        details: [],
        horseCount: 0,
        name: "社台レースホース",
        quinellaCount: 2,
        quinellaRate: 25,
        showCount: 3,
        showRate: 37.5,
        starts: 8,
        winCount: 1,
        winRate: 12.5,
      },
    ],
  });
  const request = fetchMock.mock.calls[0]?.[0];
  expect(request).toBeInstanceOf(Request);
  if (!(request instanceof Request)) throw new Error("Catalog Request expected");
  expect(request.url).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/win-rate-heatmap-stats?year=2026&month=08&day=08&keibajoCode=07&raceNumber=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=0&includeOwner=1",
  );
});

it("groups Catalog bloodline rows by category and name with sorted horse numbers", () => {
  expect(
    groupCatalogBloodlineRows([
      {
        category: "sire",
        currentHorseNumbers: "12",
        details: [],
        horseCount: 3,
        name: "ディープインパクト",
        quinellaCount: 4,
        quinellaRate: 20,
        showCount: 6,
        showRate: 30,
        starts: 20,
        winCount: 2,
        winRate: 10,
      },
      {
        category: "sire",
        currentHorseNumbers: "3",
        details: [],
        horseCount: 1,
        name: "ディープインパクト",
        quinellaCount: 4,
        quinellaRate: 20,
        showCount: 6,
        showRate: 30,
        starts: 20,
        winCount: 2,
        winRate: 10,
      },
      {
        category: "damSire",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 0,
        name: "サンデーサイレンス",
        quinellaCount: 1,
        quinellaRate: 10,
        showCount: 1,
        showRate: 10,
        starts: 10,
        winCount: 0,
        winRate: 0,
      },
    ]),
  ).toStrictEqual([
    {
      category: "sire",
      currentHorseNumbers: "3, 12",
      details: [],
      horseCount: 0,
      name: "ディープインパクト",
      quinellaCount: 4,
      quinellaRate: 20,
      showCount: 6,
      showRate: 30,
      starts: 20,
      winCount: 2,
      winRate: 10,
    },
    {
      category: "damSire",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 0,
      name: "サンデーサイレンス",
      quinellaCount: 1,
      quinellaRate: 10,
      showCount: 1,
      showRate: 10,
      starts: 10,
      winCount: 0,
      winRate: 0,
    },
  ]);
});

it("groups Catalog similar rows including owner and non-numeric horse numbers", () => {
  expect(
    groupCatalogSimilarRows([
      {
        category: "owner",
        currentHorseNumbers: "B",
        details: [],
        horseCount: 2,
        name: "社台",
        quinellaCount: 1,
        quinellaRate: 10,
        showCount: 1,
        showRate: 10,
        starts: 10,
        winCount: 1,
        winRate: 10,
      },
      {
        category: "owner",
        currentHorseNumbers: "A",
        details: [],
        horseCount: 2,
        name: "社台",
        quinellaCount: 1,
        quinellaRate: 10,
        showCount: 1,
        showRate: 10,
        starts: 10,
        winCount: 1,
        winRate: 10,
      },
      {
        category: "jockey",
        currentHorseNumbers: "2, 2",
        details: [],
        horseCount: 0,
        name: "武豊",
        quinellaCount: 3,
        quinellaRate: 30,
        showCount: 5,
        showRate: 50,
        starts: 10,
        winCount: 1,
        winRate: 10,
      },
    ]),
  ).toStrictEqual([
    {
      category: "owner",
      currentHorseNumbers: "A, B",
      details: [],
      horseCount: 0,
      name: "社台",
      quinellaCount: 1,
      quinellaRate: 10,
      showCount: 1,
      showRate: 10,
      starts: 10,
      winCount: 1,
      winRate: 10,
    },
    {
      category: "jockey",
      currentHorseNumbers: "2",
      details: [],
      horseCount: 0,
      name: "武豊",
      quinellaCount: 3,
      quinellaRate: 30,
      showCount: 5,
      showRate: 50,
      starts: 10,
      winCount: 1,
      winRate: 10,
    },
  ]);
});

it("groups empty Catalog rate rows without changing the empty list", () => {
  expect(groupCatalogBloodlineRows([])).toStrictEqual([]);
  expect(groupCatalogSimilarRows([])).toStrictEqual([]);
});

it("sorts mixed numeric horse numbers and drops blank tokens while grouping", () => {
  expect(
    groupCatalogSimilarRows([
      {
        category: "trainer",
        currentHorseNumbers: "10, , 02",
        details: [],
        horseCount: 1,
        name: "藤沢和雄",
        quinellaCount: 1,
        quinellaRate: 10,
        showCount: 1,
        showRate: 10,
        starts: 10,
        winCount: 1,
        winRate: 10,
      },
      {
        category: "trainer",
        currentHorseNumbers: "2",
        details: [],
        horseCount: 1,
        name: "藤沢和雄",
        quinellaCount: 1,
        quinellaRate: 10,
        showCount: 1,
        showRate: 10,
        starts: 10,
        winCount: 1,
        winRate: 10,
      },
    ]),
  ).toStrictEqual([
    {
      category: "trainer",
      currentHorseNumbers: "02, 2, 10",
      details: [],
      horseCount: 0,
      name: "藤沢和雄",
      quinellaCount: 1,
      quinellaRate: 10,
      showCount: 1,
      showRate: 10,
      starts: 10,
      winCount: 1,
      winRate: 10,
    },
  ]);
});

it("returns null when the Catalog binding is unavailable", async () => {
  safeGetCloudflareEnvMock.mockResolvedValue(null);
  await expect(fetchWinRateHeatmapStatsFromCatalog(query)).resolves.toBeNull();
});

it("throws when Catalog HTTP, payload, or rows are invalid", async () => {
  const fetchMock = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(new Response("unavailable", { status: 502 }))
    .mockResolvedValueOnce(Response.json({ bloodlineRows: [], similarRows: "invalid" }))
    .mockResolvedValueOnce(
      Response.json({
        bloodlineRows: [
          { category: "sire", name: "A", places: 1, shows: 1, starts: 1.5, umaban: 1, wins: 1 },
        ],
        similarRows: [],
      }),
    )
    .mockResolvedValueOnce(
      Response.json({
        bloodlineRows: [],
        similarRows: [
          { kind: "owner", name: "A", places: "A", shows: 1, starts: 1, umaban: 1, wins: 1 },
        ],
      }),
    );
  safeGetCloudflareEnvMock.mockResolvedValue({ R2_CATALOG: { fetch: fetchMock } });

  await expect(fetchWinRateHeatmapStatsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog heatmap stats failed: 502",
  );
  await expect(fetchWinRateHeatmapStatsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog heatmap stats payload is malformed",
  );
  await expect(fetchWinRateHeatmapStatsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog heatmap stats rows are malformed",
  );
  await expect(fetchWinRateHeatmapStatsFromCatalog(query)).rejects.toThrow(
    "R2 Catalog heatmap stats rows are malformed",
  );
});

it("propagates Catalog fetch failures", async () => {
  const fetchMock = vi.fn<typeof fetch>().mockRejectedValue(new Error("catalog timeout"));
  safeGetCloudflareEnvMock.mockResolvedValue({ R2_CATALOG: { fetch: fetchMock } });
  await expect(fetchWinRateHeatmapStatsFromCatalog(query)).rejects.toThrow("catalog timeout");
});
