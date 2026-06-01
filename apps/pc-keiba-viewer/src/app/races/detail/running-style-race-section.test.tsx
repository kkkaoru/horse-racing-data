// Run with: bun run test src/app/races/detail/running-style-race-section.test.tsx

import { renderToString } from "react-dom/server";
import { describe, expect, test, vi } from "vitest";

import type {
  RaceRunningStyleRow,
  RunningStyleLabel,
} from "../../../db/corner-running-style-parsers";
import type {
  RaceRowForRunningStyleBucketFilter,
  RunningStyleBucketMetrics,
  RunningStyleDimensionFlags,
} from "../../../lib/running-style-prediction-dimensions";
import type { RunningStyleBucketSectionData } from "./detail-section-data";

const getRaceRunningStylesWithCacheMock =
  vi.fn<
    (race: {
      source: string;
      kaisaiNen: string;
      kaisaiTsukihi: string;
      keibajoCode: string;
      raceBango: string;
    }) => Promise<RaceRunningStyleRow[]>
  >();
const getRunningStyleMetricsForActiveModelMock =
  vi.fn<(category: string) => Promise<{ modelVersion: string; macroF1: number | null } | null>>();
const getRunningStyleBucketSectionDataMock =
  vi.fn<
    (params: {
      day: string;
      keibajoCode: string;
      month: string;
      query: Record<string, string | string[] | undefined>;
      raceNumber: string;
      raceSource: string;
      year: string;
    }) => Promise<RunningStyleBucketSectionData>
  >();

vi.mock("../../../db/corner-running-style-queries", () => ({
  getRunningStyleMetricsForActiveModel: (category: string) =>
    getRunningStyleMetricsForActiveModelMock(category),
}));

vi.mock("../../../lib/running-style-cache.server", () => ({
  getRaceRunningStylesWithCache: (race: Parameters<typeof getRaceRunningStylesWithCacheMock>[0]) =>
    getRaceRunningStylesWithCacheMock(race),
}));

vi.mock("./detail-section-data", () => ({
  getRunningStyleBucketSectionData: (
    params: Parameters<typeof getRunningStyleBucketSectionDataMock>[0],
  ) => getRunningStyleBucketSectionDataMock(params),
}));

vi.mock("next/navigation", () => ({
  useRouter: () => ({ replace: vi.fn<(href: string) => void>() }),
  useSearchParams: () => new URLSearchParams(),
}));

vi.mock("./realtime-client", () => ({
  useRealtimeRaceSelector: <T,>(selector: (state: { payload: null }) => T): T =>
    selector({ payload: null }),
}));

const { RunningStyleRaceSection } = await import("./running-style-race-section");

const buildRow = (overrides: Partial<RaceRunningStyleRow>): RaceRunningStyleRow => ({
  bamei: "テストホース",
  category: "jra",
  horseNumber: 1,
  kaisaiNen: "2025",
  kettoTorokuBango: "h1",
  modelVersion: "jra-rs-v1.0",
  p_nige: 0.05,
  p_oikomi: 0.05,
  p_sashi: 0.4,
  p_senkou: 0.5,
  predictedAt: "2025-05-17T01:00:00Z",
  predictedLabel: "senkou" satisfies RunningStyleLabel,
  raceKey: "jra:2025:0517:05:11",
  ...overrides,
});

const EMPTY_BUCKET_SECTION_DATA: RunningStyleBucketSectionData = {
  bucketEvaluation: null,
  bucketGradeCode: null,
  bucketRace: null,
  bucketSource: null,
  dimensionFlags: null,
};

const buildBucketRace = (
  overrides: Partial<RaceRowForRunningStyleBucketFilter>,
): RaceRowForRunningStyleBucketFilter => ({
  source: "jra",
  keibajoCode: "06",
  kyori: 2500,
  kyosoShubetsuCode: "11",
  kyosoJokenCode: "999",
  kyosoJokenMeisho: "オープン",
  trackCode: "10",
  gradeCode: "A",
  kyosomeiHondai: "有馬記念",
  ...overrides,
});

const buildFlags = (
  overrides: Partial<RunningStyleDimensionFlags>,
): RunningStyleDimensionFlags => ({
  keibajo: true,
  distance: true,
  kyosoShubetsu: true,
  kyosoJoken: true,
  condition: false,
  track: true,
  grade: false,
  raceName: false,
  ...overrides,
});

const buildEvaluation = (
  overrides: Partial<RunningStyleBucketMetrics>,
): RunningStyleBucketMetrics => ({
  accuracy: 0.653,
  accuracyCI: { lower: 0.621, upper: 0.685 },
  confusionMatrix: [
    [80, 50, 15, 5],
    [70, 400, 100, 30],
    [30, 80, 350, 40],
    [10, 25, 90, 125],
  ],
  macroF1: 0.412,
  overallLogLoss: 0.875,
  perClass: {
    nige: { precision: 0.6, recall: 0.55, f1: 0.574, support: 150 },
    senkou: { precision: 0.7, recall: 0.65, f1: 0.674, support: 600 },
    sashi: { precision: 0.65, recall: 0.7, f1: 0.674, support: 500 },
    oikomi: { precision: 0.4, recall: 0.3, f1: 0.343, support: 250 },
  },
  perClassLogLoss: { nige: 0.9, senkou: 0.7, sashi: 0.85, oikomi: 1.2 },
  predictionCount: 1500,
  qwk: 0.715,
  raceCount: 120,
  smallSampleWarning: false,
  top2Accuracy: 0.842,
  weightedF1: 0.638,
  ...overrides,
});

describe("RunningStyleRaceSection", () => {
  test("renders ban-ei category using nar metrics", async () => {
    getRaceRunningStylesWithCacheMock.mockReset();
    getRunningStyleMetricsForActiveModelMock.mockReset();
    getRunningStyleBucketSectionDataMock.mockReset();
    getRaceRunningStylesWithCacheMock.mockResolvedValue([
      buildRow({
        category: "ban-ei",
        modelVersion: "ban-ei-rs-v1.0",
        predictedLabel: "nige",
        raceKey: "nar:2025:0517:83:01",
      }),
    ]);
    getRunningStyleMetricsForActiveModelMock.mockResolvedValue({
      macroF1: 0.39,
      modelVersion: "nar-rs-v1.0",
    });
    getRunningStyleBucketSectionDataMock.mockResolvedValue(EMPTY_BUCKET_SECTION_DATA);
    const element = await RunningStyleRaceSection({
      category: "ban-ei",
      day: "17",
      kaisaiNen: "2025",
      kaisaiTsukihi: "0517",
      keibajoCode: "83",
      month: "05",
      raceBango: "01",
      raceNumber: "01",
      runnersByUmaban: {},
      searchParams: {},
      source: "nar",
      year: "2025",
    });
    const html = renderToString(element);
    expect(html).toContain("脚質予測");
    expect(html).toContain("nar-rs-v1.0");
    expect(getRaceRunningStylesWithCacheMock).toHaveBeenCalledWith({
      kaisaiNen: "2025",
      kaisaiTsukihi: "0517",
      keibajoCode: "83",
      raceBango: "01",
      source: "nar",
    });
    expect(getRunningStyleMetricsForActiveModelMock).toHaveBeenCalledWith("nar");
  });

  test("loads D1 rows and metrics in parallel and renders the section", async () => {
    getRaceRunningStylesWithCacheMock.mockReset();
    getRunningStyleMetricsForActiveModelMock.mockReset();
    getRunningStyleBucketSectionDataMock.mockReset();
    getRaceRunningStylesWithCacheMock.mockResolvedValue([buildRow({})]);
    getRunningStyleMetricsForActiveModelMock.mockResolvedValue({
      macroF1: 0.42,
      modelVersion: "jra-rs-v1.0",
    });
    getRunningStyleBucketSectionDataMock.mockResolvedValue(EMPTY_BUCKET_SECTION_DATA);
    const element = await RunningStyleRaceSection({
      category: "jra",
      day: "17",
      kaisaiNen: "2025",
      kaisaiTsukihi: "0517",
      keibajoCode: "05",
      month: "05",
      raceBango: "11",
      raceNumber: "11",
      runnersByUmaban: {},
      searchParams: {},
      source: "jra",
      year: "2025",
    });
    const html = renderToString(element);
    expect(html).toContain("脚質予測");
    expect(html).toContain("テストホース");
    expect(html).toContain("jra-rs-v1.0");
    expect(getRaceRunningStylesWithCacheMock).toHaveBeenCalledWith({
      kaisaiNen: "2025",
      kaisaiTsukihi: "0517",
      keibajoCode: "05",
      raceBango: "11",
      source: "jra",
    });
    expect(getRunningStyleMetricsForActiveModelMock).toHaveBeenCalledWith("jra");
  });

  test("renders the empty state when no rows are returned", async () => {
    getRaceRunningStylesWithCacheMock.mockReset();
    getRunningStyleMetricsForActiveModelMock.mockReset();
    getRunningStyleBucketSectionDataMock.mockReset();
    getRaceRunningStylesWithCacheMock.mockResolvedValue([]);
    getRunningStyleMetricsForActiveModelMock.mockResolvedValue(null);
    getRunningStyleBucketSectionDataMock.mockResolvedValue(EMPTY_BUCKET_SECTION_DATA);
    const element = await RunningStyleRaceSection({
      category: "nar",
      day: "28",
      kaisaiNen: "2025",
      kaisaiTsukihi: "0228",
      keibajoCode: "42",
      month: "02",
      raceBango: "07",
      raceNumber: "07",
      runnersByUmaban: {},
      searchParams: {},
      source: "nar",
      year: "2025",
    });
    const html = renderToString(element);
    expect(html).toContain("このレースの脚質予測データはまだありません");
  });

  test("renders the empty state when D1 running-style data is unavailable", async () => {
    getRaceRunningStylesWithCacheMock.mockReset();
    getRunningStyleMetricsForActiveModelMock.mockReset();
    getRunningStyleBucketSectionDataMock.mockReset();
    getRaceRunningStylesWithCacheMock.mockRejectedValue(
      new Error("D1_ERROR: no such table: race_running_styles: SQLITE_ERROR"),
    );
    getRunningStyleMetricsForActiveModelMock.mockRejectedValue(new Error("missing model table"));
    getRunningStyleBucketSectionDataMock.mockResolvedValue(EMPTY_BUCKET_SECTION_DATA);
    const element = await RunningStyleRaceSection({
      category: "nar",
      day: "18",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0518",
      keibajoCode: "35",
      month: "05",
      raceBango: "01",
      raceNumber: "01",
      runnersByUmaban: {},
      searchParams: {},
      source: "nar",
      year: "2026",
    });
    const html = renderToString(element);
    expect(html).toContain("このレースの脚質予測データはまだありません");
  });

  test("renders bucket evaluation panel when bucket section data resolves with metrics", async () => {
    getRaceRunningStylesWithCacheMock.mockReset();
    getRunningStyleMetricsForActiveModelMock.mockReset();
    getRunningStyleBucketSectionDataMock.mockReset();
    getRaceRunningStylesWithCacheMock.mockResolvedValue([buildRow({})]);
    getRunningStyleMetricsForActiveModelMock.mockResolvedValue({
      macroF1: 0.42,
      modelVersion: "jra-rs-v1.0",
    });
    getRunningStyleBucketSectionDataMock.mockResolvedValue({
      bucketEvaluation: buildEvaluation({}),
      bucketGradeCode: "A",
      bucketRace: buildBucketRace({}),
      bucketSource: "jra",
      dimensionFlags: buildFlags({}),
    });
    const element = await RunningStyleRaceSection({
      category: "jra",
      day: "22",
      kaisaiNen: "2024",
      kaisaiTsukihi: "1222",
      keibajoCode: "06",
      month: "12",
      raceBango: "11",
      raceNumber: "11",
      runnersByUmaban: {},
      searchParams: {},
      source: "jra",
      year: "2024",
    });
    const html = renderToString(element);
    expect(html).toContain("同条件 bucket での検証精度");
    expect(html).toContain("65.3%");
    expect(getRunningStyleBucketSectionDataMock).toHaveBeenCalledWith({
      day: "22",
      keibajoCode: "06",
      month: "12",
      query: {},
      raceNumber: "11",
      raceSource: "jra",
      year: "2024",
    });
  });

  test("propagates searchParams to the bucket section data fetch", async () => {
    getRaceRunningStylesWithCacheMock.mockReset();
    getRunningStyleMetricsForActiveModelMock.mockReset();
    getRunningStyleBucketSectionDataMock.mockReset();
    getRaceRunningStylesWithCacheMock.mockResolvedValue([buildRow({})]);
    getRunningStyleMetricsForActiveModelMock.mockResolvedValue(null);
    getRunningStyleBucketSectionDataMock.mockResolvedValue(EMPTY_BUCKET_SECTION_DATA);
    await RunningStyleRaceSection({
      category: "jra",
      day: "22",
      kaisaiNen: "2024",
      kaisaiTsukihi: "1222",
      keibajoCode: "06",
      month: "12",
      raceBango: "11",
      raceNumber: "11",
      runnersByUmaban: {},
      searchParams: { runningStyleDistance: "0" },
      source: "jra",
      year: "2024",
    });
    expect(getRunningStyleBucketSectionDataMock).toHaveBeenCalledWith({
      day: "22",
      keibajoCode: "06",
      month: "12",
      query: { runningStyleDistance: "0" },
      raceNumber: "11",
      raceSource: "jra",
      year: "2024",
    });
  });

  test("falls back to empty bucket data when the bucket section fetch rejects", async () => {
    getRaceRunningStylesWithCacheMock.mockReset();
    getRunningStyleMetricsForActiveModelMock.mockReset();
    getRunningStyleBucketSectionDataMock.mockReset();
    getRaceRunningStylesWithCacheMock.mockResolvedValue([buildRow({})]);
    getRunningStyleMetricsForActiveModelMock.mockResolvedValue(null);
    getRunningStyleBucketSectionDataMock.mockRejectedValue(new Error("PG_ERROR: connection lost"));
    const element = await RunningStyleRaceSection({
      category: "jra",
      day: "22",
      kaisaiNen: "2024",
      kaisaiTsukihi: "1222",
      keibajoCode: "06",
      month: "12",
      raceBango: "11",
      raceNumber: "11",
      runnersByUmaban: {},
      searchParams: {},
      source: "jra",
      year: "2024",
    });
    const html = renderToString(element);
    expect(html).toContain("テストホース");
    expect(html).not.toContain("同条件 bucket での検証精度");
  });
});
