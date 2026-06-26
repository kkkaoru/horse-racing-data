// Run with bun. `bun run --filter pc-keiba-viewer test`
import { beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const mocks = vi.hoisted(() => ({
  buildDetailSectionCacheKeyMock: vi.fn<(...args: never[]) => unknown>(),
  buildFinishPredictionInputsCacheKeyMock: vi.fn<(...args: never[]) => unknown>(),
  buildStaleDetailSectionResponseMock: vi.fn<(...args: never[]) => unknown>(),
  getCachedDetailSectionResponseMock: vi.fn<(...args: never[]) => unknown>(),
  getCachedFinishPredictionInputsMock: vi.fn<(...args: never[]) => unknown>(),
  getDetailSectionPayloadMock: vi.fn<(...args: never[]) => unknown>(),
  getFinishPositionBucketSectionDataMock: vi.fn<(...args: never[]) => unknown>(),
  getRaceDetailMock: vi.fn<(...args: never[]) => unknown>(),
  getRaceSourceByRouteMock: vi.fn<(...args: never[]) => unknown>(),
  getStaleDetailSectionBodyMock: vi.fn<(...args: never[]) => unknown>(),
  isDefaultDetailSectionCacheRequestMock: vi.fn<(...args: never[]) => unknown>(),
  putDetailSectionCacheMock: vi.fn<(...args: never[]) => unknown>(),
  putFinishPredictionInputsCacheMock: vi.fn<(...args: never[]) => unknown>(),
  safeGetCloudflareExecutionContextMock: vi.fn<() => Promise<unknown>>(),
  stripDetailSectionCacheWarmParamsMock: vi.fn<(...args: never[]) => unknown>(),
}));

vi.mock("../../../../../../../../../../db/queries", () => ({
  getRaceDetail: mocks.getRaceDetailMock,
  getRaceSourceByRoute: mocks.getRaceSourceByRouteMock,
}));

vi.mock("../../../../../../../../../../lib/cloudflare-context.server", () => ({
  safeGetCloudflareExecutionContext: mocks.safeGetCloudflareExecutionContextMock,
}));

vi.mock("../../../../../../../../../../lib/finish-prediction-inputs-cache.server", () => ({
  buildFinishPredictionInputsCacheKey: mocks.buildFinishPredictionInputsCacheKeyMock,
  getCachedFinishPredictionInputs: mocks.getCachedFinishPredictionInputsMock,
  putFinishPredictionInputsCache: mocks.putFinishPredictionInputsCacheMock,
}));

vi.mock("../../../../../../../../../../lib/race-detail-section-cache", () => ({
  buildDetailSectionCacheKey: mocks.buildDetailSectionCacheKeyMock,
  isDefaultDetailSectionCacheRequest: mocks.isDefaultDetailSectionCacheRequestMock,
  PREDICTION_REFRESH_PARAM: "__predictionRefresh",
  stripDetailSectionCacheWarmParams: mocks.stripDetailSectionCacheWarmParamsMock,
}));

vi.mock("../../../../../../../../../../lib/race-detail-section-cache.server", () => ({
  buildStaleDetailSectionResponse: mocks.buildStaleDetailSectionResponseMock,
  getCachedDetailSectionResponse: mocks.getCachedDetailSectionResponseMock,
  getStaleDetailSectionBody: mocks.getStaleDetailSectionBodyMock,
  putDetailSectionCache: mocks.putDetailSectionCacheMock,
}));

vi.mock("../../../../../../../../../races/detail/detail-section-data", () => ({
  getDetailSectionPayload: mocks.getDetailSectionPayloadMock,
  getFinishPositionBucketSectionData: mocks.getFinishPositionBucketSectionDataMock,
}));

const {
  buildDetailSectionCacheKeyMock,
  buildFinishPredictionInputsCacheKeyMock,
  getCachedDetailSectionResponseMock,
  getCachedFinishPredictionInputsMock,
  getDetailSectionPayloadMock,
  getFinishPositionBucketSectionDataMock,
  getRaceSourceByRouteMock,
  getStaleDetailSectionBodyMock,
  isDefaultDetailSectionCacheRequestMock,
  stripDetailSectionCacheWarmParamsMock,
} = mocks;

import { GET } from "./route";

beforeEach(() => {
  vi.resetAllMocks();
  // Default stub: pass-through to URLSearchParams so route logic sees the real query.
  stripDetailSectionCacheWarmParamsMock.mockImplementation(
    (params: URLSearchParams) => new URLSearchParams(params),
  );
  // Default stub: not cacheable by section-level default cache.
  isDefaultDetailSectionCacheRequestMock.mockReturnValue(false);
  buildDetailSectionCacheKeyMock.mockReturnValue("unused-default-cache-key");
  buildFinishPredictionInputsCacheKeyMock.mockReturnValue("finish-prediction-cache-key");
  getCachedDetailSectionResponseMock.mockResolvedValue(null);
  getStaleDetailSectionBodyMock.mockResolvedValue(null);
});

it("returns fresh bucket merged with cached static payload when finish-prediction cache hits and raceSource is truthy", async () => {
  getCachedFinishPredictionInputsMock.mockResolvedValue({
    evaluation: {
      category: "jra-graded",
      categoryLabel: "JRA G",
      fromDate: "2025-01-01",
      pairScore: 25.5,
      place1Accuracy: 30.1,
      place2Accuracy: 45.2,
      place3Accuracy: 60.3,
      raceCount: 1000,
      target: "place1",
      toDate: "2025-12-31",
      top1Accuracy: 30.1,
      top3BoxAccuracy: 60.0,
      top3ExactOrderAccuracy: 7.5,
      top3PlaceRelation: 0.5,
      top3WinnerCapture: 0.7,
      top5WinnerCapture: 0.9,
    },
    inputs: {
      currentDistance: 1600,
      currentGradeCode: null,
      currentKeibajoCode: "43",
      currentKyosoJokenCode: "010",
      currentKyosoJokenMeisho: "1勝",
      currentRaceDate: "20260603",
      currentSource: "nar",
      currentTrackCode: "10",
      modelPredictionFeatures: [],
      results: [],
      runners: [],
      sameDayVenueJockeyWins: [],
      similarityFeatures: [],
    },
  });
  getRaceSourceByRouteMock.mockResolvedValue("nar");
  getFinishPositionBucketSectionDataMock.mockResolvedValue({
    bucketEvaluation: null,
    bucketGradeCode: null,
    bucketModelVersion: null,
    bucketRace: null,
    bucketScope: null,
    bucketSource: null,
  });
  const request = new Request(
    "https://example.com/api/races/2026/06/03/43/12/sections/finish-prediction",
  );
  const response = await GET(request, {
    params: Promise.resolve({
      day: "03",
      keibajoCode: "43",
      month: "06",
      raceNumber: "12",
      section: "finish-prediction",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
    bucket: {
      bucketEvaluation: null,
      bucketGradeCode: null,
      bucketModelVersion: null,
      bucketRace: null,
      bucketScope: null,
      bucketSource: null,
    },
    evaluation: {
      category: "jra-graded",
      categoryLabel: "JRA G",
      fromDate: "2025-01-01",
      pairScore: 25.5,
      place1Accuracy: 30.1,
      place2Accuracy: 45.2,
      place3Accuracy: 60.3,
      raceCount: 1000,
      target: "place1",
      toDate: "2025-12-31",
      top1Accuracy: 30.1,
      top3BoxAccuracy: 60.0,
      top3ExactOrderAccuracy: 7.5,
      top3PlaceRelation: 0.5,
      top3WinnerCapture: 0.7,
      top5WinnerCapture: 0.9,
    },
    inputs: {
      currentDistance: 1600,
      currentGradeCode: null,
      currentKeibajoCode: "43",
      currentKyosoJokenCode: "010",
      currentKyosoJokenMeisho: "1勝",
      currentRaceDate: "20260603",
      currentSource: "nar",
      currentTrackCode: "10",
      modelPredictionFeatures: [],
      results: [],
      runners: [],
      sameDayVenueJockeyWins: [],
      similarityFeatures: [],
    },
    type: "finish-prediction",
  });
  expect(getFinishPositionBucketSectionDataMock).toHaveBeenCalledTimes(1);
});

it("falls through to compute path and returns 404 when finish-prediction cache hits but raceSource is null", async () => {
  getCachedFinishPredictionInputsMock.mockResolvedValue({
    evaluation: {
      category: "jra-graded",
      categoryLabel: "JRA G",
      fromDate: "2025-01-01",
      pairScore: 25.5,
      place1Accuracy: 30.1,
      place2Accuracy: 45.2,
      place3Accuracy: 60.3,
      raceCount: 1000,
      target: "place1",
      toDate: "2025-12-31",
      top1Accuracy: 30.1,
      top3BoxAccuracy: 60.0,
      top3ExactOrderAccuracy: 7.5,
      top3PlaceRelation: 0.5,
      top3WinnerCapture: 0.7,
      top5WinnerCapture: 0.9,
    },
    inputs: {
      currentDistance: 1600,
      currentGradeCode: null,
      currentKeibajoCode: "43",
      currentKyosoJokenCode: "010",
      currentKyosoJokenMeisho: "1勝",
      currentRaceDate: "20260603",
      currentSource: "nar",
      currentTrackCode: "10",
      modelPredictionFeatures: [],
      results: [],
      runners: [],
      sameDayVenueJockeyWins: [],
      similarityFeatures: [],
    },
  });
  getRaceSourceByRouteMock.mockResolvedValue(null);
  getDetailSectionPayloadMock.mockResolvedValue(null);
  const request = new Request(
    "https://example.com/api/races/2026/06/03/43/12/sections/finish-prediction",
  );
  const response = await GET(request, {
    params: Promise.resolve({
      day: "03",
      keibajoCode: "43",
      month: "06",
      raceNumber: "12",
      section: "finish-prediction",
      year: "2026",
    }),
  });
  expect(response.status).toBe(404);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({ error: "not_found" });
  expect(getFinishPositionBucketSectionDataMock).not.toHaveBeenCalled();
});

it("returns full compute payload with bucket field when finish-prediction static cache misses", async () => {
  getCachedFinishPredictionInputsMock.mockResolvedValue(null);
  getRaceSourceByRouteMock.mockResolvedValue("nar");
  getDetailSectionPayloadMock.mockResolvedValue({
    bucket: {
      bucketEvaluation: null,
      bucketGradeCode: null,
      bucketModelVersion: null,
      bucketRace: null,
      bucketScope: null,
      bucketSource: null,
    },
    evaluation: {
      category: "jra-graded",
      categoryLabel: "JRA G",
      fromDate: "2025-01-01",
      pairScore: 25.5,
      place1Accuracy: 30.1,
      place2Accuracy: 45.2,
      place3Accuracy: 60.3,
      raceCount: 1000,
      target: "place1",
      toDate: "2025-12-31",
      top1Accuracy: 30.1,
      top3BoxAccuracy: 60.0,
      top3ExactOrderAccuracy: 7.5,
      top3PlaceRelation: 0.5,
      top3WinnerCapture: 0.7,
      top5WinnerCapture: 0.9,
    },
    inputs: {
      currentDistance: 1600,
      currentGradeCode: null,
      currentKeibajoCode: "43",
      currentKyosoJokenCode: "010",
      currentKyosoJokenMeisho: "1勝",
      currentRaceDate: "20260603",
      currentSource: "nar",
      currentTrackCode: "10",
      modelPredictionFeatures: [],
      results: [],
      runners: [],
      sameDayVenueJockeyWins: [],
      similarityFeatures: [],
    },
    type: "finish-prediction",
  });
  const request = new Request(
    "https://example.com/api/races/2026/06/03/43/12/sections/finish-prediction",
  );
  const response = await GET(request, {
    params: Promise.resolve({
      day: "03",
      keibajoCode: "43",
      month: "06",
      raceNumber: "12",
      section: "finish-prediction",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
    bucket: {
      bucketEvaluation: null,
      bucketGradeCode: null,
      bucketModelVersion: null,
      bucketRace: null,
      bucketScope: null,
      bucketSource: null,
    },
    evaluation: {
      category: "jra-graded",
      categoryLabel: "JRA G",
      fromDate: "2025-01-01",
      pairScore: 25.5,
      place1Accuracy: 30.1,
      place2Accuracy: 45.2,
      place3Accuracy: 60.3,
      raceCount: 1000,
      target: "place1",
      toDate: "2025-12-31",
      top1Accuracy: 30.1,
      top3BoxAccuracy: 60.0,
      top3ExactOrderAccuracy: 7.5,
      top3PlaceRelation: 0.5,
      top3WinnerCapture: 0.7,
      top5WinnerCapture: 0.9,
    },
    inputs: {
      currentDistance: 1600,
      currentGradeCode: null,
      currentKeibajoCode: "43",
      currentKyosoJokenCode: "010",
      currentKyosoJokenMeisho: "1勝",
      currentRaceDate: "20260603",
      currentSource: "nar",
      currentTrackCode: "10",
      modelPredictionFeatures: [],
      results: [],
      runners: [],
      sameDayVenueJockeyWins: [],
      similarityFeatures: [],
    },
    type: "finish-prediction",
  });
});

it("skips finish-prediction inputs cache read but still writes when __predictionRefresh is present", async () => {
  stripDetailSectionCacheWarmParamsMock.mockImplementation((params: URLSearchParams) => {
    const next = new URLSearchParams(params);
    next.delete("__predictionRefresh");
    return next;
  });
  getRaceSourceByRouteMock.mockResolvedValue("nar");
  mocks.getRaceDetailMock.mockResolvedValue({ raceId: "race-detail" });
  getDetailSectionPayloadMock.mockResolvedValue({
    evaluation: {
      category: "jra-graded",
      categoryLabel: "JRA G",
      fromDate: "2025-01-01",
      pairScore: 25.5,
      place1Accuracy: 30.1,
      place2Accuracy: 45.2,
      place3Accuracy: 60.3,
      raceCount: 1000,
      target: "place1",
      toDate: "2025-12-31",
      top1Accuracy: 30.1,
      top3BoxAccuracy: 60.0,
      top3ExactOrderAccuracy: 7.5,
      top3PlaceRelation: 0.5,
      top3WinnerCapture: 0.7,
      top5WinnerCapture: 0.9,
    },
    inputs: {
      currentDistance: 1600,
      currentGradeCode: null,
      currentKeibajoCode: "43",
      currentKyosoJokenCode: "010",
      currentKyosoJokenMeisho: "1勝",
      currentRaceDate: "20260603",
      currentSource: "nar",
      currentTrackCode: "10",
      modelPredictionFeatures: [
        {
          horseNumber: "01",
          modelVersion: "iter20",
          predictedFinishNorm: 0.12,
          showProbability: 0.4,
          winProbability: 0.2,
        },
      ],
      results: [],
      runners: [],
      sameDayVenueJockeyWins: [],
      similarityFeatures: [],
    },
    type: "finish-prediction",
  });
  mocks.putFinishPredictionInputsCacheMock.mockResolvedValue(undefined);
  const request = new Request(
    "https://example.com/api/races/2026/06/03/43/12/sections/finish-prediction?__predictionRefresh=1",
  );
  const response = await GET(request, {
    params: Promise.resolve({
      day: "03",
      keibajoCode: "43",
      month: "06",
      raceNumber: "12",
      section: "finish-prediction",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  expect(getCachedFinishPredictionInputsMock).not.toHaveBeenCalled();
  expect(mocks.putFinishPredictionInputsCacheMock).toHaveBeenCalledTimes(1);
});

it("skips finish-prediction cache shortcut when section is ability", async () => {
  getRaceSourceByRouteMock.mockResolvedValue("nar");
  getDetailSectionPayloadMock.mockResolvedValue({
    abilityTests: [],
    type: "ability",
  });
  const request = new Request("https://example.com/api/races/2026/06/03/43/12/sections/ability");
  const response = await GET(request, {
    params: Promise.resolve({
      day: "03",
      keibajoCode: "43",
      month: "06",
      raceNumber: "12",
      section: "ability",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
    abilityTests: [],
    type: "ability",
  });
  expect(getCachedFinishPredictionInputsMock).not.toHaveBeenCalled();
  expect(getFinishPositionBucketSectionDataMock).not.toHaveBeenCalled();
});

it("skips finish-prediction inputs cache write when modelPredictionFeatures is empty", async () => {
  getCachedFinishPredictionInputsMock.mockResolvedValue(null);
  getRaceSourceByRouteMock.mockResolvedValue("nar");
  mocks.getRaceDetailMock.mockResolvedValue({ raceId: "race-detail" });
  getDetailSectionPayloadMock.mockResolvedValue({
    bucket: {
      bucketEvaluation: null,
      bucketGradeCode: null,
      bucketModelVersion: null,
      bucketRace: null,
      bucketScope: null,
      bucketSource: null,
    },
    evaluation: {
      category: "jra-graded",
      categoryLabel: "JRA G",
      fromDate: "2025-01-01",
      pairScore: 25.5,
      place1Accuracy: 30.1,
      place2Accuracy: 45.2,
      place3Accuracy: 60.3,
      raceCount: 1000,
      target: "place1",
      toDate: "2025-12-31",
      top1Accuracy: 30.1,
      top3BoxAccuracy: 60.0,
      top3ExactOrderAccuracy: 7.5,
      top3PlaceRelation: 0.5,
      top3WinnerCapture: 0.7,
      top5WinnerCapture: 0.9,
    },
    inputs: {
      currentDistance: 1600,
      currentGradeCode: null,
      currentKeibajoCode: "43",
      currentKyosoJokenCode: "010",
      currentKyosoJokenMeisho: "1勝",
      currentRaceDate: "20260603",
      currentSource: "nar",
      currentTrackCode: "10",
      modelPredictionFeatures: [],
      results: [],
      runners: [],
      sameDayVenueJockeyWins: [],
      similarityFeatures: [],
    },
    type: "finish-prediction",
  });
  mocks.putFinishPredictionInputsCacheMock.mockResolvedValue(undefined);
  const request = new Request(
    "https://example.com/api/races/2026/06/03/43/12/sections/finish-prediction",
  );
  const response = await GET(request, {
    params: Promise.resolve({
      day: "03",
      keibajoCode: "43",
      month: "06",
      raceNumber: "12",
      section: "finish-prediction",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  expect(mocks.putFinishPredictionInputsCacheMock).not.toHaveBeenCalled();
});

it("bypasses section cache read and stale SWR for finish-prediction when __predictionRefresh is present", async () => {
  stripDetailSectionCacheWarmParamsMock.mockImplementation((params: URLSearchParams) => {
    const next = new URLSearchParams(params);
    next.delete("__predictionRefresh");
    return next;
  });
  isDefaultDetailSectionCacheRequestMock.mockReturnValue(true);
  buildDetailSectionCacheKeyMock.mockReturnValue("section-cache-key");
  getStaleDetailSectionBodyMock.mockResolvedValue("stale-body");
  getRaceSourceByRouteMock.mockResolvedValue("nar");
  mocks.getRaceDetailMock.mockResolvedValue({ raceId: "race-detail" });
  getDetailSectionPayloadMock.mockResolvedValue({
    evaluation: {
      category: "jra-graded",
      categoryLabel: "JRA G",
      fromDate: "2025-01-01",
      pairScore: 25.5,
      place1Accuracy: 30.1,
      place2Accuracy: 45.2,
      place3Accuracy: 60.3,
      raceCount: 1000,
      target: "place1",
      toDate: "2025-12-31",
      top1Accuracy: 30.1,
      top3BoxAccuracy: 60.0,
      top3ExactOrderAccuracy: 7.5,
      top3PlaceRelation: 0.5,
      top3WinnerCapture: 0.7,
      top5WinnerCapture: 0.9,
    },
    inputs: {
      currentDistance: 1600,
      currentGradeCode: null,
      currentKeibajoCode: "43",
      currentKyosoJokenCode: "010",
      currentKyosoJokenMeisho: "1勝",
      currentRaceDate: "20260603",
      currentSource: "nar",
      currentTrackCode: "10",
      modelPredictionFeatures: [
        {
          horseNumber: "01",
          modelVersion: "iter20",
          predictedFinishNorm: 0.12,
          showProbability: 0.4,
          winProbability: 0.2,
        },
      ],
      results: [],
      runners: [],
      sameDayVenueJockeyWins: [],
      similarityFeatures: [],
    },
    type: "finish-prediction",
  });
  mocks.putFinishPredictionInputsCacheMock.mockResolvedValue(undefined);
  mocks.putDetailSectionCacheMock.mockResolvedValue(undefined);
  const request = new Request(
    "https://example.com/api/races/2026/06/03/43/12/sections/finish-prediction?__predictionRefresh=1",
  );
  const response = await GET(request, {
    params: Promise.resolve({
      day: "03",
      keibajoCode: "43",
      month: "06",
      raceNumber: "12",
      section: "finish-prediction",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  expect(getCachedDetailSectionResponseMock).not.toHaveBeenCalled();
  expect(mocks.buildStaleDetailSectionResponseMock).not.toHaveBeenCalled();
});

it("returns cached finish-prediction section response when cached body has model prediction features", async () => {
  isDefaultDetailSectionCacheRequestMock.mockReturnValue(true);
  buildDetailSectionCacheKeyMock.mockReturnValue("section-cache-key");
  getCachedFinishPredictionInputsMock.mockResolvedValue(null);
  getCachedDetailSectionResponseMock.mockResolvedValue(
    new Response(
      '{"type":"finish-prediction","inputs":{"modelPredictionFeatures":[{"horseNumber":"01"}]}}',
      { status: 200 },
    ),
  );
  const request = new Request(
    "https://example.com/api/races/2026/06/03/43/12/sections/finish-prediction",
  );
  const response = await GET(request, {
    params: Promise.resolve({
      day: "03",
      keibajoCode: "43",
      month: "06",
      raceNumber: "12",
      section: "finish-prediction",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
    inputs: { modelPredictionFeatures: [{ horseNumber: "01" }] },
    type: "finish-prediction",
  });
  expect(getDetailSectionPayloadMock).not.toHaveBeenCalled();
});

it("recomputes finish-prediction when cached section body lacks model prediction features", async () => {
  isDefaultDetailSectionCacheRequestMock.mockReturnValue(true);
  buildDetailSectionCacheKeyMock.mockReturnValue("section-cache-key");
  getCachedFinishPredictionInputsMock.mockResolvedValue(null);
  getCachedDetailSectionResponseMock.mockResolvedValue(
    new Response('{"type":"finish-prediction","inputs":{"modelPredictionFeatures":[]}}', {
      status: 200,
    }),
  );
  getStaleDetailSectionBodyMock.mockResolvedValue(null);
  getRaceSourceByRouteMock.mockResolvedValue("nar");
  mocks.getRaceDetailMock.mockResolvedValue({ raceId: "race-detail" });
  getDetailSectionPayloadMock.mockResolvedValue({
    evaluation: {
      category: "jra-graded",
      categoryLabel: "JRA G",
      fromDate: "2025-01-01",
      pairScore: 25.5,
      place1Accuracy: 30.1,
      place2Accuracy: 45.2,
      place3Accuracy: 60.3,
      raceCount: 1000,
      target: "place1",
      toDate: "2025-12-31",
      top1Accuracy: 30.1,
      top3BoxAccuracy: 60.0,
      top3ExactOrderAccuracy: 7.5,
      top3PlaceRelation: 0.5,
      top3WinnerCapture: 0.7,
      top5WinnerCapture: 0.9,
    },
    inputs: {
      currentDistance: 1600,
      currentGradeCode: null,
      currentKeibajoCode: "43",
      currentKyosoJokenCode: "010",
      currentKyosoJokenMeisho: "1勝",
      currentRaceDate: "20260603",
      currentSource: "nar",
      currentTrackCode: "10",
      modelPredictionFeatures: [
        {
          horseNumber: "01",
          modelVersion: "iter20",
          predictedFinishNorm: 0.12,
          showProbability: 0.4,
          winProbability: 0.2,
        },
      ],
      results: [],
      runners: [],
      sameDayVenueJockeyWins: [],
      similarityFeatures: [],
    },
    type: "finish-prediction",
  });
  mocks.putFinishPredictionInputsCacheMock.mockResolvedValue(undefined);
  mocks.putDetailSectionCacheMock.mockResolvedValue(undefined);
  const request = new Request(
    "https://example.com/api/races/2026/06/03/43/12/sections/finish-prediction",
  );
  const response = await GET(request, {
    params: Promise.resolve({
      day: "03",
      keibajoCode: "43",
      month: "06",
      raceNumber: "12",
      section: "finish-prediction",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  expect(getDetailSectionPayloadMock).toHaveBeenCalledTimes(1);
});

it("serves stale finish-prediction snapshot when stale body has model prediction features", async () => {
  isDefaultDetailSectionCacheRequestMock.mockReturnValue(true);
  buildDetailSectionCacheKeyMock.mockReturnValue("section-cache-key");
  getCachedFinishPredictionInputsMock.mockResolvedValue(null);
  getCachedDetailSectionResponseMock.mockResolvedValue(null);
  getStaleDetailSectionBodyMock.mockResolvedValue(
    '{"type":"finish-prediction","inputs":{"modelPredictionFeatures":[{"horseNumber":"01"}]}}',
  );
  mocks.buildStaleDetailSectionResponseMock.mockReturnValue(
    new Response("stale-finish-payload", { status: 200 }),
  );
  const request = new Request(
    "https://example.com/api/races/2026/06/03/43/12/sections/finish-prediction",
  );
  const response = await GET(request, {
    params: Promise.resolve({
      day: "03",
      keibajoCode: "43",
      month: "06",
      raceNumber: "12",
      section: "finish-prediction",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  const body = await response.text();
  expect(body).toBe("stale-finish-payload");
  expect(mocks.buildStaleDetailSectionResponseMock).toHaveBeenCalledTimes(1);
  expect(getDetailSectionPayloadMock).not.toHaveBeenCalled();
});

it("skips stale finish-prediction snapshot and recomputes when stale body lacks model prediction features", async () => {
  isDefaultDetailSectionCacheRequestMock.mockReturnValue(true);
  buildDetailSectionCacheKeyMock.mockReturnValue("section-cache-key");
  getCachedFinishPredictionInputsMock.mockResolvedValue(null);
  getCachedDetailSectionResponseMock.mockResolvedValue(null);
  getStaleDetailSectionBodyMock.mockResolvedValue(
    '{"type":"finish-prediction","inputs":{"modelPredictionFeatures":[]}}',
  );
  getRaceSourceByRouteMock.mockResolvedValue("nar");
  mocks.getRaceDetailMock.mockResolvedValue({ raceId: "race-detail" });
  getDetailSectionPayloadMock.mockResolvedValue({
    evaluation: {
      category: "jra-graded",
      categoryLabel: "JRA G",
      fromDate: "2025-01-01",
      pairScore: 25.5,
      place1Accuracy: 30.1,
      place2Accuracy: 45.2,
      place3Accuracy: 60.3,
      raceCount: 1000,
      target: "place1",
      toDate: "2025-12-31",
      top1Accuracy: 30.1,
      top3BoxAccuracy: 60.0,
      top3ExactOrderAccuracy: 7.5,
      top3PlaceRelation: 0.5,
      top3WinnerCapture: 0.7,
      top5WinnerCapture: 0.9,
    },
    inputs: {
      currentDistance: 1600,
      currentGradeCode: null,
      currentKeibajoCode: "43",
      currentKyosoJokenCode: "010",
      currentKyosoJokenMeisho: "1勝",
      currentRaceDate: "20260603",
      currentSource: "nar",
      currentTrackCode: "10",
      modelPredictionFeatures: [
        {
          horseNumber: "01",
          modelVersion: "iter20",
          predictedFinishNorm: 0.12,
          showProbability: 0.4,
          winProbability: 0.2,
        },
      ],
      results: [],
      runners: [],
      sameDayVenueJockeyWins: [],
      similarityFeatures: [],
    },
    type: "finish-prediction",
  });
  mocks.putFinishPredictionInputsCacheMock.mockResolvedValue(undefined);
  mocks.putDetailSectionCacheMock.mockResolvedValue(undefined);
  const request = new Request(
    "https://example.com/api/races/2026/06/03/43/12/sections/finish-prediction",
  );
  const response = await GET(request, {
    params: Promise.resolve({
      day: "03",
      keibajoCode: "43",
      month: "06",
      raceNumber: "12",
      section: "finish-prediction",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  expect(mocks.buildStaleDetailSectionResponseMock).not.toHaveBeenCalled();
  expect(getDetailSectionPayloadMock).toHaveBeenCalledTimes(1);
});
