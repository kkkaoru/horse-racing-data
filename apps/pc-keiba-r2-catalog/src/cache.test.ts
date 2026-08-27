import { expect, it, vi } from "vitest";

import {
  cacheRequestFor,
  featureDescriptor,
  conditionHistoryStatsDescriptor,
  heatmapStatsDescriptor,
  horseRaceResultsDescriptor,
  jsonRowsResponse,
  kvKeyFor,
  parsePositiveSeconds,
  populateCacheApi,
  populateCaches,
  purgeDescriptors,
  raceEntityRecentResultsDescriptor,
  readKvConditionHistoryStats,
  readKvHeatmapStats,
  readKvRows,
  trainingDescriptor,
} from "./cache";
import type { CacheStore, KvStore } from "./types";

const cacheMocks = (): {
  cache: CacheStore;
  deleteMock: ReturnType<typeof vi.fn>;
  putMock: ReturnType<typeof vi.fn>;
} => {
  const deleteMock = vi.fn(async () => true);
  const putMock = vi.fn(async () => undefined);
  return {
    cache: {
      delete: deleteMock,
      match: vi.fn(async () => undefined),
      put: putMock,
    },
    deleteMock,
    putMock,
  };
};

const kvMocks = (
  value: string | null,
): { deleteMock: ReturnType<typeof vi.fn>; kv: KvStore; putMock: ReturnType<typeof vi.fn> } => {
  const deleteMock = vi.fn(async () => undefined);
  const putMock = vi.fn(async () => undefined);
  return {
    deleteMock,
    kv: { delete: deleteMock, get: vi.fn(async () => value), put: putMock },
    putMock,
  };
};

it("compacts signed entity cursors below the KV key limit", () => {
  const descriptor = raceEntityRecentResultsDescriptor({
    cursor: "x".repeat(500),
    date: "20260827",
    entityType: "jockey",
    horseNumber: "07",
    keibajoCode: "50",
    limit: 30,
    raceBango: "05",
    source: "nar",
  });
  expect(cacheRequestFor(descriptor).url).toMatch(/cursor=x{64}$/u);
  expect(new TextEncoder().encode(kvKeyFor(descriptor)).byteLength).toBeLessThan(512);
});

it("builds canonical Cache API and KV keys", () => {
  const descriptor = featureDescriptor({
    date: "20260715",
    keibajoCode: "05",
    raceBango: "01",
    source: "jra",
  });
  expect(cacheRequestFor(descriptor).url).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/race-features?date=20260715&source=jra&keibajoCode=05&raceBango=01",
  );
  expect(kvKeyFor(descriptor)).toBe(
    "catalog:v2:v2/race-features?date=20260715&source=jra&keibajoCode=05&raceBango=01",
  );
  expect(cacheRequestFor({ date: "20260715", kind: "race-keys" }).url).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/race-keys?date=20260715&schema=grade-v1",
  );
  const training = trainingDescriptor({
    date: "20260715",
    keibajoCode: "05",
    raceBango: "01",
  });
  expect(cacheRequestFor(training).url).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/race-trainings?date=20260715&keibajoCode=05&raceBango=01",
  );
  expect(kvKeyFor(training)).toBe(
    "catalog:v2:v2/race-trainings?date=20260715&keibajoCode=05&raceBango=01",
  );
  const heatmap = heatmapStatsDescriptor({
    date: "20260715",
    includeDistance: true,
    includeSurface: false,
    includeTurn: true,
    includeVenue: true,
    keibajoCode: "05",
    raceBango: "01",
    source: "jra",
    years: 10,
  });
  expect(cacheRequestFor(heatmap).url).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/win-rate-heatmap-stats?date=20260715&keibajoCode=05&raceBango=01&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=0&includeTurn=1&nameTrim=ideographic&emptyTurnBypass=1&ungradedOp=1&emptyGradeMatch=2",
  );
  expect(kvKeyFor(heatmap)).toBe(
    "catalog:v2:v2/win-rate-heatmap-stats?date=20260715&keibajoCode=05&raceBango=01&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=0&includeTurn=1&nameTrim=ideographic&emptyTurnBypass=1&ungradedOp=1&emptyGradeMatch=2",
  );
  expect(
    cacheRequestFor(
      heatmapStatsDescriptor({
        date: "20260715",
        includeDistance: false,
        includeSurface: false,
        includeTurn: false,
        includeVenue: false,
        keibajoCode: "06",
        raceBango: "11",
        source: "nar",
        years: 5,
      }),
    ).url,
  ).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/win-rate-heatmap-stats?date=20260715&keibajoCode=06&raceBango=11&source=nar&years=5&includeVenue=0&includeDistance=0&includeSurface=0&includeTurn=0&nameTrim=ideographic&emptyTurnBypass=1&ungradedOp=1&emptyGradeMatch=2",
  );
  expect(
    cacheRequestFor(
      heatmapStatsDescriptor({
        date: "20260715",
        includeDistance: true,
        includeOwner: true,
        includeSurface: false,
        includeTurn: true,
        includeVenue: true,
        keibajoCode: "05",
        raceBango: "01",
        source: "jra",
        years: 10,
      }),
    ).url,
  ).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/win-rate-heatmap-stats?date=20260715&keibajoCode=05&raceBango=01&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=0&includeTurn=1&nameTrim=ideographic&emptyTurnBypass=1&includeOwner=1&ungradedOp=1&emptyGradeMatch=2",
  );
  expect(
    cacheRequestFor(
      heatmapStatsDescriptor({
        date: "20260715",
        includeDistance: true,
        includeJockeyFrame: true,
        includeOwner: false,
        includeSurface: false,
        includeTurn: true,
        includeVenue: true,
        keibajoCode: "05",
        raceBango: "01",
        source: "jra",
        years: 10,
      }),
    ).url,
  ).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/win-rate-heatmap-stats?date=20260715&keibajoCode=05&raceBango=01&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=0&includeTurn=1&nameTrim=ideographic&emptyTurnBypass=1&includeJockeyFrame=1&ungradedOp=1&emptyGradeMatch=2",
  );
  expect(
    cacheRequestFor(
      heatmapStatsDescriptor({
        date: "20260715",
        includeDistance: true,
        includeJockeyFrame: true,
        includeOwner: true,
        includeSurface: false,
        includeTurn: true,
        includeVenue: true,
        keibajoCode: "05",
        raceBango: "01",
        source: "jra",
        years: 10,
      }),
    ).url,
  ).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/win-rate-heatmap-stats?date=20260715&keibajoCode=05&raceBango=01&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=0&includeTurn=1&nameTrim=ideographic&emptyTurnBypass=1&includeOwner=1&includeJockeyFrame=1&ungradedOp=1&emptyGradeMatch=2",
  );
  const horseResults = horseRaceResultsDescriptor({
    date: "20260715",
    keibajoCode: "05",
    raceBango: "01",
    source: "jra",
    sourceScope: "all",
  });
  expect(cacheRequestFor(horseResults).url).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/horse-race-results?date=20260715&keibajoCode=05&raceBango=01&source=jra&sourceScope=all",
  );
  expect(
    cacheRequestFor(
      conditionHistoryStatsDescriptor({
        date: "20260715",
        includeDistance: true,
        includeSurface: false,
        includeTurn: true,
        includeVenue: true,
        keibajoCode: "05",
        raceBango: "01",
        source: "jra",
        years: 10,
      }),
    ).url,
  ).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/condition-history-stats?date=20260715&keibajoCode=05&raceBango=01&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=0&includeTurn=1&targetRaces=1&finishDetails=1&ungradedOp=1&emptyGradeMatch=2",
  );
  expect(
    cacheRequestFor(
      conditionHistoryStatsDescriptor({
        date: "20260715",
        includeDistance: false,
        includeSurface: false,
        includeTurn: false,
        includeVenue: false,
        keibajoCode: "83",
        raceBango: "09",
        source: "nar",
        years: 5,
      }),
    ).url,
  ).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/condition-history-stats?date=20260715&keibajoCode=83&raceBango=09&source=nar&years=5&includeVenue=0&includeDistance=0&includeSurface=0&includeTurn=0&targetRaces=1&finishDetails=1&ungradedOp=1&emptyGradeMatch=2",
  );
  expect(
    cacheRequestFor(
      heatmapStatsDescriptor({
        date: "20260715",
        includeAge: true,
        includeClass: true,
        includeConditionKey: true,
        includeDistance: true,
        includeGrade: true,
        includeRaceTitle: true,
        includeSurface: false,
        includeTrackCode: true,
        includeTurn: true,
        includeVenue: true,
        keibajoCode: "05",
        raceBango: "01",
        source: "jra",
        years: 10,
      }),
    ).url,
  ).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/win-rate-heatmap-stats?date=20260715&keibajoCode=05&raceBango=01&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=0&includeTurn=1&nameTrim=ideographic&emptyTurnBypass=1&ungradedOp=1&emptyGradeMatch=2&includeGrade=1&includeTrackCode=1&includeAge=1&includeClass=1&includeConditionKey=1&includeRaceTitle=1",
  );
  expect(
    cacheRequestFor(
      conditionHistoryStatsDescriptor({
        date: "20260715",
        includeAge: true,
        includeClass: true,
        includeConditionKey: true,
        includeDistance: true,
        includeGrade: true,
        includeRaceTitle: true,
        includeSurface: false,
        includeTrackCode: true,
        includeTurn: true,
        includeVenue: true,
        keibajoCode: "05",
        raceBango: "01",
        source: "jra",
        years: 10,
      }),
    ).url,
  ).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/condition-history-stats?date=20260715&keibajoCode=05&raceBango=01&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=0&includeTurn=1&targetRaces=1&finishDetails=1&ungradedOp=1&emptyGradeMatch=2&includeGrade=1&includeTrackCode=1&includeAge=1&includeClass=1&includeConditionKey=1&includeRaceTitle=1",
  );
});

it("uses only positive integer TTL values", () => {
  expect(parsePositiveSeconds("30", 60)).toBe(30);
  expect(parsePositiveSeconds("0", 60)).toBe(60);
  expect(parsePositiveSeconds("2.5", 60)).toBe(60);
  expect(parsePositiveSeconds(undefined, 60)).toBe(60);
});

it("reads only valid rows envelopes from KV", async () => {
  await expect(readKvRows(kvMocks('{"rows":[]}').kv, "key")).resolves.toBe('{"rows":[]}');
  await expect(readKvRows(kvMocks("[]").kv, "key")).resolves.toBe(null);
  await expect(readKvRows(kvMocks("{}").kv, "key")).resolves.toBe(null);
  await expect(readKvRows(kvMocks("bad").kv, "key")).resolves.toBe(null);
  await expect(readKvRows(kvMocks(null).kv, "key")).resolves.toBe(null);
});

it("reads only condition-history-stats envelopes from KV", async () => {
  const valid =
    '{"frameStats":[],"weightClassStats":[],"carriedWeightClassStats":[],"finishPositionStats":[],"raceTimeStats":{}}';
  await expect(readKvConditionHistoryStats(kvMocks(valid).kv, "key")).resolves.toBe(valid);
  await expect(readKvConditionHistoryStats(kvMocks('{"rows":[]}').kv, "key")).resolves.toBe(null);
  await expect(
    readKvConditionHistoryStats(kvMocks('{"frameStats":[],"weightClassStats":[]}').kv, "key"),
  ).resolves.toBe(null);
  await expect(
    readKvConditionHistoryStats(
      kvMocks(
        '{"frameStats":[],"weightClassStats":[],"carriedWeightClassStats":[],"finishPositionStats":[],"raceTimeStats":[]}',
      ).kv,
      "key",
    ),
  ).resolves.toBe(null);
  await expect(readKvConditionHistoryStats(kvMocks("[]").kv, "key")).resolves.toBe(null);
  await expect(readKvConditionHistoryStats(kvMocks("bad").kv, "key")).resolves.toBe(null);
  await expect(readKvConditionHistoryStats(kvMocks(null).kv, "key")).resolves.toBe(null);
});

it("reads only heatmap-stats envelopes from KV", async () => {
  await expect(
    readKvHeatmapStats(kvMocks('{"bloodlineRows":[],"similarRows":[]}').kv, "key"),
  ).resolves.toBe('{"bloodlineRows":[],"similarRows":[]}');
  await expect(readKvHeatmapStats(kvMocks('{"rows":[]}').kv, "key")).resolves.toBe(null);
  await expect(readKvHeatmapStats(kvMocks('{"bloodlineRows":[]}').kv, "key")).resolves.toBe(null);
  await expect(readKvHeatmapStats(kvMocks('{"similarRows":[]}').kv, "key")).resolves.toBe(null);
  await expect(readKvHeatmapStats(kvMocks("[]").kv, "key")).resolves.toBe(null);
  await expect(readKvHeatmapStats(kvMocks("bad").kv, "key")).resolves.toBe(null);
  await expect(readKvHeatmapStats(kvMocks(null).kv, "key")).resolves.toBe(null);
});

it("creates cache responses and writes Cache API plus KV", async () => {
  const cacheState = cacheMocks();
  const kvState = kvMocks(null);
  const descriptor = { date: "20260715", kind: "race-keys" } satisfies Parameters<
    typeof populateCaches
  >[2];
  await populateCaches(cacheState.cache, kvState.kv, descriptor, '{"rows":[]}', 60, 600);
  expect(cacheState.putMock).toHaveBeenCalledOnce();
  expect(kvState.putMock).toHaveBeenCalledWith(
    "catalog:v2:v2/race-keys?date=20260715&schema=grade-v1",
    '{"rows":[]}',
    { expirationTtl: 600 },
  );
  const storedResponse = cacheState.putMock.mock.calls[0]?.[1];
  expect(storedResponse?.headers.get("Cache-Control")).toBe("public, max-age=60");
  await expect(storedResponse?.text()).resolves.toBe('{"rows":[]}');
  expect(jsonRowsResponse('{"rows":[]}', "kv").headers.get("X-Catalog-Cache")).toBe("kv");
});

it("contains cache write failures and purges both cache tiers", async () => {
  const cacheState = cacheMocks();
  const kvState = kvMocks(null);
  cacheState.putMock.mockRejectedValueOnce(new Error("cache unavailable"));
  kvState.putMock.mockRejectedValueOnce(new Error("kv unavailable"));
  await expect(
    populateCaches(
      cacheState.cache,
      kvState.kv,
      { date: "20260715", kind: "race-keys" },
      '{"rows":[]}',
      60,
      600,
    ),
  ).resolves.toBe(undefined);
  await expect(
    purgeDescriptors(cacheState.cache, kvState.kv, [
      { date: "20260715", kind: "race-keys" },
      { date: "20260715", kind: "race-features", source: "all" },
    ]),
  ).resolves.toBe(2);
  expect(cacheState.deleteMock).toHaveBeenCalledTimes(2);
  expect(kvState.deleteMock).toHaveBeenCalledTimes(2);
});

it("writes a single Cache API response", async () => {
  const cacheState = cacheMocks();
  const request = new Request("https://cache.test/item");
  await populateCacheApi(cacheState.cache, request, "[1]", 15);
  expect(cacheState.putMock).toHaveBeenCalledOnce();
});
