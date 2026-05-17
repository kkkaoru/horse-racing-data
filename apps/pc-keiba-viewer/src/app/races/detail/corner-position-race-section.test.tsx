// Run with: bun run test src/app/races/detail/corner-position-race-section.test.tsx

import { renderToString } from "react-dom/server";
import { describe, expect, test, vi } from "vitest";

import type { RaceCornerPositionRow } from "../../../db/corner-running-style-queries";

const getActiveCornerPositionModelMock =
  vi.fn<
    (
      category: string,
    ) => Promise<{ modelVersion: string; category: string; activatedAt: Date } | null>
  >();
const getRaceCornerPositionPredictionsMock = vi.fn<
  (
    keys: {
      source: string;
      kaisaiNen: string;
      kaisaiTsukihi: string;
      keibajoCode: string;
      raceBango: string;
    },
    modelVersion: string,
  ) => Promise<RaceCornerPositionRow[]>
>();
const getCornerPositionMetricsForActiveModelMock =
  vi.fn<(category: string) => Promise<{ meanMae: number | null; modelVersion: string } | null>>();

vi.mock("../../../db/corner-running-style-queries", () => ({
  getActiveCornerPositionModel: (category: string) => getActiveCornerPositionModelMock(category),
  getCornerPositionMetricsForActiveModel: (category: string) =>
    getCornerPositionMetricsForActiveModelMock(category),
  getRaceCornerPositionPredictions: (
    keys: {
      source: string;
      kaisaiNen: string;
      kaisaiTsukihi: string;
      keibajoCode: string;
      raceBango: string;
    },
    modelVersion: string,
  ) => getRaceCornerPositionPredictionsMock(keys, modelVersion),
}));

const { CornerPositionRaceSection } = await import("./corner-position-race-section");

const buildRow = (overrides: Partial<RaceCornerPositionRow>): RaceCornerPositionRow => ({
  corner1Pred: 0.0,
  corner3Pred: 0.0,
  corner4Pred: 0.0,
  kaisaiNen: "2025",
  kaisaiTsukihi: "0517",
  keibajoCode: "05",
  kettoTorokuBango: "h1",
  modelVersion: "jra-corner-v1.0",
  raceBango: "11",
  source: "jra",
  umaban: 1,
  ...overrides,
});

describe("CornerPositionRaceSection", () => {
  test("returns null for ban-ei category", async () => {
    const element = await CornerPositionRaceSection({
      bameiByUmaban: {},
      category: "ban-ei",
      isStraightCourse: false,
      kaisaiNen: "2025",
      kaisaiTsukihi: "0517",
      keibajoCode: "83",
      raceBango: "01",
      source: "nar",
    });
    expect(element).toBe(null);
  });

  test("renders straight-course banner without touching the DB", async () => {
    getActiveCornerPositionModelMock.mockReset();
    getRaceCornerPositionPredictionsMock.mockReset();
    const element = await CornerPositionRaceSection({
      bameiByUmaban: {},
      category: "jra",
      isStraightCourse: true,
      kaisaiNen: "2025",
      kaisaiTsukihi: "0517",
      keibajoCode: "01",
      raceBango: "11",
      source: "jra",
    });
    const html = renderToString(element);
    expect(html).toContain("直線コース（コーナーなし、参考値）");
    expect(getActiveCornerPositionModelMock).not.toHaveBeenCalled();
    expect(getRaceCornerPositionPredictionsMock).not.toHaveBeenCalled();
  });

  test("renders empty state when no active model is registered", async () => {
    getActiveCornerPositionModelMock.mockReset();
    getActiveCornerPositionModelMock.mockResolvedValue(null);
    getRaceCornerPositionPredictionsMock.mockReset();
    const element = await CornerPositionRaceSection({
      bameiByUmaban: { 1: "テスト" },
      category: "jra",
      isStraightCourse: false,
      kaisaiNen: "2025",
      kaisaiTsukihi: "0517",
      keibajoCode: "05",
      raceBango: "11",
      source: "jra",
    });
    const html = renderToString(element);
    expect(html).toContain("このレースのコーナー予測データはまだありません");
    expect(getRaceCornerPositionPredictionsMock).not.toHaveBeenCalled();
  });

  test("renders empty state when active model lookup is unavailable", async () => {
    getActiveCornerPositionModelMock.mockReset();
    getActiveCornerPositionModelMock.mockRejectedValue(new Error("missing active model table"));
    getRaceCornerPositionPredictionsMock.mockReset();
    const element = await CornerPositionRaceSection({
      bameiByUmaban: { 1: "テスト" },
      category: "jra",
      isStraightCourse: false,
      kaisaiNen: "2026",
      kaisaiTsukihi: "0518",
      keibajoCode: "35",
      raceBango: "01",
      source: "nar",
    });
    const html = renderToString(element);
    expect(html).toContain("このレースのコーナー予測データはまだありません");
    expect(getRaceCornerPositionPredictionsMock).not.toHaveBeenCalled();
  });

  test("loads predictions and metrics in parallel and forwards bamei map", async () => {
    getActiveCornerPositionModelMock.mockReset();
    getRaceCornerPositionPredictionsMock.mockReset();
    getCornerPositionMetricsForActiveModelMock.mockReset();
    getActiveCornerPositionModelMock.mockResolvedValue({
      activatedAt: new Date("2025-05-17T01:00:00Z"),
      category: "jra",
      modelVersion: "jra-corner-v1.0",
    });
    getRaceCornerPositionPredictionsMock.mockResolvedValue([buildRow({ umaban: 1 })]);
    getCornerPositionMetricsForActiveModelMock.mockResolvedValue({
      meanMae: 1.42,
      modelVersion: "jra-corner-v1.0",
    });
    const element = await CornerPositionRaceSection({
      bameiByUmaban: { 1: "馬A" },
      category: "jra",
      isStraightCourse: false,
      kaisaiNen: "2025",
      kaisaiTsukihi: "0517",
      keibajoCode: "05",
      raceBango: "11",
      source: "jra",
    });
    const html = renderToString(element);
    expect(html).toContain("コーナー通過順予測");
    expect(html).toContain("馬A");
    expect(html).toContain("jra-corner-v1.0");
    expect(getRaceCornerPositionPredictionsMock).toHaveBeenCalledWith(
      {
        kaisaiNen: "2025",
        kaisaiTsukihi: "0517",
        keibajoCode: "05",
        raceBango: "11",
        source: "jra",
      },
      "jra-corner-v1.0",
    );
  });

  test("renders empty state when corner prediction data is unavailable", async () => {
    getActiveCornerPositionModelMock.mockReset();
    getRaceCornerPositionPredictionsMock.mockReset();
    getCornerPositionMetricsForActiveModelMock.mockReset();
    getActiveCornerPositionModelMock.mockResolvedValue({
      activatedAt: new Date("2025-05-17T01:00:00Z"),
      category: "jra",
      modelVersion: "jra-corner-v1.0",
    });
    getRaceCornerPositionPredictionsMock.mockRejectedValue(
      new Error("relation race_corner_position_predictions does not exist"),
    );
    getCornerPositionMetricsForActiveModelMock.mockRejectedValue(
      new Error("missing metrics table"),
    );
    const element = await CornerPositionRaceSection({
      bameiByUmaban: { 1: "馬A" },
      category: "jra",
      isStraightCourse: false,
      kaisaiNen: "2026",
      kaisaiTsukihi: "0518",
      keibajoCode: "35",
      raceBango: "01",
      source: "nar",
    });
    const html = renderToString(element);
    expect(html).toContain("このレースのコーナー予測データはまだありません");
  });
});
