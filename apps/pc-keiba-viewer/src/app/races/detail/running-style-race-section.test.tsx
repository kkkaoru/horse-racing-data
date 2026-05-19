// Run with: bun run test src/app/races/detail/running-style-race-section.test.tsx

import { renderToString } from "react-dom/server";
import { describe, expect, test, vi } from "vitest";

import type {
  RaceRunningStyleRow,
  RunningStyleLabel,
} from "../../../db/corner-running-style-parsers";

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

vi.mock("../../../db/corner-running-style-queries", () => ({
  getRunningStyleMetricsForActiveModel: (category: string) =>
    getRunningStyleMetricsForActiveModelMock(category),
}));

vi.mock("../../../lib/running-style-cache.server", () => ({
  getRaceRunningStylesWithCache: (race: Parameters<typeof getRaceRunningStylesWithCacheMock>[0]) =>
    getRaceRunningStylesWithCacheMock(race),
}));

vi.mock("next/navigation", () => ({
  useRouter: () => ({ replace: vi.fn<(href: string) => void>() }),
  useSearchParams: () => new URLSearchParams(),
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
  raceKey: "jra:20250517:05:11",
  ...overrides,
});

describe("RunningStyleRaceSection", () => {
  test("returns null for ban-ei category", async () => {
    const element = await RunningStyleRaceSection({
      category: "ban-ei",
      kaisaiNen: "2025",
      kaisaiTsukihi: "0517",
      keibajoCode: "83",
      raceBango: "01",
      runnersByUmaban: {},
      source: "nar",
    });
    expect(element).toBe(null);
  });

  test("loads D1 rows and metrics in parallel and renders the section", async () => {
    getRaceRunningStylesWithCacheMock.mockReset();
    getRunningStyleMetricsForActiveModelMock.mockReset();
    getRaceRunningStylesWithCacheMock.mockResolvedValue([buildRow({})]);
    getRunningStyleMetricsForActiveModelMock.mockResolvedValue({
      macroF1: 0.42,
      modelVersion: "jra-rs-v1.0",
    });
    const element = await RunningStyleRaceSection({
      category: "jra",
      kaisaiNen: "2025",
      kaisaiTsukihi: "0517",
      keibajoCode: "05",
      raceBango: "11",
      runnersByUmaban: {},
      source: "jra",
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
    getRaceRunningStylesWithCacheMock.mockResolvedValue([]);
    getRunningStyleMetricsForActiveModelMock.mockResolvedValue(null);
    const element = await RunningStyleRaceSection({
      category: "nar",
      kaisaiNen: "2025",
      kaisaiTsukihi: "0228",
      keibajoCode: "42",
      raceBango: "07",
      runnersByUmaban: {},
      source: "nar",
    });
    const html = renderToString(element);
    expect(html).toContain("このレースの脚質予測データはまだありません");
  });

  test("renders the empty state when D1 running-style data is unavailable", async () => {
    getRaceRunningStylesWithCacheMock.mockReset();
    getRunningStyleMetricsForActiveModelMock.mockReset();
    getRaceRunningStylesWithCacheMock.mockRejectedValue(
      new Error("D1_ERROR: no such table: race_running_styles: SQLITE_ERROR"),
    );
    getRunningStyleMetricsForActiveModelMock.mockRejectedValue(new Error("missing model table"));
    const element = await RunningStyleRaceSection({
      category: "nar",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0518",
      keibajoCode: "35",
      raceBango: "01",
      runnersByUmaban: {},
      source: "nar",
    });
    const html = renderToString(element);
    expect(html).toContain("このレースの脚質予測データはまだありません");
  });
});
