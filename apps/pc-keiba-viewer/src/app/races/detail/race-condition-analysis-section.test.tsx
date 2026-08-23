// bun で実行する (bunx vitest)
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import type {
  FinishPositionStatsRow,
  PayoutStatsRow,
  RaceTimeStats,
} from "../../../lib/race-types";
import {
  loadConditionFinishChartForCurrentUser,
  loadConditionPayoutChartForCurrentUser,
  persistConditionFinishChartForCurrentUser,
  persistConditionPayoutChartForCurrentUser,
} from "../../../lib/user-preferences-indexeddb";
import { RaceConditionAnalysisSection } from "./race-condition-analysis-section";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.mocked(loadConditionPayoutChartForCurrentUser).mockReset();
  vi.mocked(loadConditionFinishChartForCurrentUser).mockReset();
  vi.mocked(persistConditionPayoutChartForCurrentUser).mockReset();
  vi.mocked(persistConditionFinishChartForCurrentUser).mockReset();
  vi.mocked(loadConditionPayoutChartForCurrentUser).mockResolvedValue(true);
  vi.mocked(loadConditionFinishChartForCurrentUser).mockResolvedValue(true);
  vi.mocked(persistConditionPayoutChartForCurrentUser).mockResolvedValue(undefined);
  vi.mocked(persistConditionFinishChartForCurrentUser).mockResolvedValue(undefined);
});

vi.mock("next/navigation", () => ({
  useRouter: () => ({
    replace: vi.fn<() => void>(),
  }),
}));

vi.mock("../../../lib/user-preferences-indexeddb", () => ({
  loadConditionFinishChartForCurrentUser: vi.fn<() => Promise<boolean>>(async () => true),
  loadConditionPayoutChartForCurrentUser: vi.fn<() => Promise<boolean>>(async () => true),
  persistConditionFinishChartForCurrentUser: vi.fn<(showChart: boolean) => Promise<void>>(
    async () => undefined,
  ),
  persistConditionPayoutChartForCurrentUser: vi.fn<(showChart: boolean) => Promise<void>>(
    async () => undefined,
  ),
}));

const raceTimeStats: RaceTimeStats = {
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
};

it("does not render condition filters, time-trend records, or the former frame-order table", () => {
  vi.stubGlobal(
    "matchMedia",
    vi.fn(() => ({
      addEventListener: vi.fn<() => void>(),
      addListener: vi.fn<() => void>(),
      matches: false,
      removeEventListener: vi.fn<() => void>(),
      removeListener: vi.fn<() => void>(),
    })),
  );
  render(
    <RaceConditionAnalysisSection
      afterTargetRaces={null}
      finishPositionStats={[]}
      payoutStats={[]}
      raceTimeStats={raceTimeStats}
    />,
  );
  expect(screen.queryByRole("heading", { name: "枠順分析" })).toBeNull();
  expect(screen.queryByText("枠順分析")).toBeNull();
  expect(screen.queryByText("条件設定")).toBeNull();
  expect(screen.queryByText("全ての条件を外す")).toBeNull();
  expect(screen.queryByRole("heading", { name: "タイム傾向" })).toBeNull();
  expect(screen.queryByText("最速レースタイム")).toBeNull();
  expect(screen.queryByText("該当する最速レースはありません。")).toBeNull();
  expect(screen.getByRole("heading", { name: "対象レース一覧" })).toBeDefined();
  expect(screen.getByRole("heading", { name: "払い戻し傾向" })).toBeDefined();
  expect(screen.getByRole("heading", { name: "着順別 人気・オッズ" })).toBeDefined();
  const finishHeading = screen.getByRole("heading", { name: "着順別 人気・オッズ" });
  const payoutHeading = screen.getByRole("heading", { name: "払い戻し傾向" });
  expect(finishHeading.compareDocumentPosition(payoutHeading)).toStrictEqual(4);
});

it("renders afterTargetRaces immediately after the target race list", () => {
  vi.stubGlobal(
    "matchMedia",
    vi.fn(() => ({
      addEventListener: vi.fn<() => void>(),
      addListener: vi.fn<() => void>(),
      matches: false,
      removeEventListener: vi.fn<() => void>(),
      removeListener: vi.fn<() => void>(),
    })),
  );
  render(
    <RaceConditionAnalysisSection
      afterTargetRaces={<div data-testid="heatmap-slot">heatmap</div>}
      finishPositionStats={[]}
      payoutStats={[]}
      raceTimeStats={raceTimeStats}
    />,
  );
  const targetHeading = screen.getByRole("heading", { name: "対象レース一覧" });
  const heatmapSlot = screen.getByTestId("heatmap-slot");
  const finishHeading = screen.getByRole("heading", { name: "着順別 人気・オッズ" });
  const payoutHeading = screen.getByRole("heading", { name: "払い戻し傾向" });
  expect(targetHeading.compareDocumentPosition(heatmapSlot)).toStrictEqual(4);
  expect(heatmapSlot.compareDocumentPosition(finishHeading)).toStrictEqual(4);
  expect(finishHeading.compareDocumentPosition(payoutHeading)).toStrictEqual(4);
});

const payoutStats: PayoutStatsRow[] = [
  {
    averagePayout: 1200,
    betType: "単勝",
    count: 8,
    details: [],
    maxPayout: 4000,
    medianPayout: 450,
    minPayout: 200,
  },
];

const finishPositionStats: FinishPositionStatsRow[] = [
  {
    averageOdds: 6.2,
    averagePopularity: 3.1,
    count: 8,
    details: [
      {
        date: "20260111",
        frameNumber: "1",
        horseName: "Alpha",
        horseNumber: "01",
        jockeyName: "Jockey A",
        keibajoCode: "05",
        popularity: "1",
        raceName: "一般",
        raceNumber: "01",
        raceTime: "1123",
        rank: "01",
        winOdds: "25",
      },
    ],
    finishPosition: 1,
    medianOdds: 4.8,
    medianPopularity: 2,
  },
];

it("shows payout and finish charts by default instead of the tables", () => {
  render(
    <RaceConditionAnalysisSection
      afterTargetRaces={null}
      finishPositionStats={finishPositionStats}
      payoutStats={payoutStats}
      raceTimeStats={raceTimeStats}
    />,
  );
  expect(screen.getByRole("figure", { name: "払い戻し傾向グラフ" })).toBeDefined();
  expect(screen.getByRole("figure", { name: "着順別人気オッズグラフ" })).toBeDefined();
  expect(screen.queryByRole("columnheader", { name: "馬券" })).toBeNull();
  expect(screen.queryByRole("columnheader", { name: "着順" })).toBeNull();
  const payoutToggle = screen.getByRole("group", { name: "払い戻し傾向の表示" });
  const finishToggle = screen.getByRole("group", { name: "着順別人気オッズの表示" });
  expect(within(payoutToggle).getByRole("radio", { name: "グラフ" })).toHaveProperty(
    "checked",
    true,
  );
  expect(within(payoutToggle).getByRole("radio", { name: "テキスト" })).toHaveProperty(
    "checked",
    false,
  );
  expect(within(finishToggle).getByRole("radio", { name: "グラフ" })).toHaveProperty(
    "checked",
    true,
  );
  expect(within(finishToggle).getByRole("radio", { name: "テキスト" })).toHaveProperty(
    "checked",
    false,
  );
  expect(within(payoutToggle).getByRole("radio", { name: "グラフ" })).toHaveProperty(
    "name",
    "condition-payout-view",
  );
  expect(within(finishToggle).getByRole("radio", { name: "グラフ" })).toHaveProperty(
    "name",
    "condition-finish-view",
  );
});

it("switches the payout section to the table when テキスト is checked", () => {
  render(
    <RaceConditionAnalysisSection
      afterTargetRaces={null}
      finishPositionStats={[]}
      payoutStats={payoutStats}
      raceTimeStats={raceTimeStats}
    />,
  );
  const payoutToggle = screen.getByRole("group", { name: "払い戻し傾向の表示" });
  fireEvent.click(within(payoutToggle).getByRole("radio", { name: "テキスト" }));
  expect(within(payoutToggle).getByRole("radio", { name: "テキスト" })).toHaveProperty(
    "checked",
    true,
  );
  expect(within(payoutToggle).getByRole("radio", { name: "グラフ" })).toHaveProperty(
    "checked",
    false,
  );
  expect(screen.getByRole("columnheader", { name: "馬券" })).toBeDefined();
  expect(screen.queryByRole("figure", { name: "払い戻し傾向グラフ" })).toBeNull();
  expect(vi.mocked(persistConditionPayoutChartForCurrentUser).mock.calls).toStrictEqual([[false]]);
  fireEvent.click(within(payoutToggle).getByRole("radio", { name: "グラフ" }));
  expect(within(payoutToggle).getByRole("radio", { name: "グラフ" })).toHaveProperty(
    "checked",
    true,
  );
  expect(within(payoutToggle).getByRole("radio", { name: "テキスト" })).toHaveProperty(
    "checked",
    false,
  );
  expect(screen.getByRole("figure", { name: "払い戻し傾向グラフ" })).toBeDefined();
  expect(screen.queryByRole("columnheader", { name: "馬券" })).toBeNull();
  expect(vi.mocked(persistConditionPayoutChartForCurrentUser).mock.calls).toStrictEqual([
    [false],
    [true],
  ]);
});

it("keeps the payout chart on when preference load fails", async () => {
  vi.mocked(loadConditionPayoutChartForCurrentUser).mockRejectedValue(new Error("idb unavailable"));
  render(
    <RaceConditionAnalysisSection
      afterTargetRaces={null}
      finishPositionStats={[]}
      payoutStats={payoutStats}
      raceTimeStats={raceTimeStats}
    />,
  );
  await act(async () => undefined);
  expect(screen.getByRole("figure", { name: "払い戻し傾向グラフ" })).toBeDefined();
});

it("restores the payout chart preference from the current user", async () => {
  vi.mocked(loadConditionPayoutChartForCurrentUser).mockResolvedValue(false);
  render(
    <RaceConditionAnalysisSection
      afterTargetRaces={null}
      finishPositionStats={[]}
      payoutStats={payoutStats}
      raceTimeStats={raceTimeStats}
    />,
  );
  await waitFor(() => {
    expect(screen.getByRole("columnheader", { name: "馬券" })).toBeDefined();
  });
});

it("keeps a payout toggle made before preference load finishes", async () => {
  const load: { resolve: (value: boolean) => void } = {
    resolve: (_value: boolean) => undefined,
  };
  vi.mocked(loadConditionPayoutChartForCurrentUser).mockImplementation(
    () =>
      new Promise((resolve) => {
        load.resolve = resolve;
      }),
  );
  render(
    <RaceConditionAnalysisSection
      afterTargetRaces={null}
      finishPositionStats={[]}
      payoutStats={payoutStats}
      raceTimeStats={raceTimeStats}
    />,
  );
  const payoutToggle = screen.getByRole("group", { name: "払い戻し傾向の表示" });
  fireEvent.click(within(payoutToggle).getByRole("radio", { name: "テキスト" }));
  expect(screen.getByRole("columnheader", { name: "馬券" })).toBeDefined();
  load.resolve(true);
  await act(async () => undefined);
  expect(screen.getByRole("columnheader", { name: "馬券" })).toBeDefined();
});

it("switches the finish section to the table when テキスト is checked", () => {
  render(
    <RaceConditionAnalysisSection
      afterTargetRaces={null}
      finishPositionStats={finishPositionStats}
      payoutStats={[]}
      raceTimeStats={raceTimeStats}
    />,
  );
  const finishToggle = screen.getByRole("group", { name: "着順別人気オッズの表示" });
  fireEvent.click(within(finishToggle).getByRole("radio", { name: "テキスト" }));
  expect(within(finishToggle).getByRole("radio", { name: "テキスト" })).toHaveProperty(
    "checked",
    true,
  );
  expect(within(finishToggle).getByRole("radio", { name: "グラフ" })).toHaveProperty(
    "checked",
    false,
  );
  expect(screen.getByRole("columnheader", { name: "着順" })).toBeDefined();
  expect(screen.queryByRole("figure", { name: "着順別人気オッズグラフ" })).toBeNull();
  expect(vi.mocked(persistConditionFinishChartForCurrentUser).mock.calls).toStrictEqual([[false]]);
  fireEvent.click(within(finishToggle).getByRole("radio", { name: "グラフ" }));
  expect(within(finishToggle).getByRole("radio", { name: "グラフ" })).toHaveProperty(
    "checked",
    true,
  );
  expect(within(finishToggle).getByRole("radio", { name: "テキスト" })).toHaveProperty(
    "checked",
    false,
  );
  expect(screen.getByRole("figure", { name: "着順別人気オッズグラフ" })).toBeDefined();
  expect(screen.queryByRole("columnheader", { name: "着順" })).toBeNull();
  expect(vi.mocked(persistConditionFinishChartForCurrentUser).mock.calls).toStrictEqual([
    [false],
    [true],
  ]);
});

it("draws the finish chart from catalog rows that only have aggregate odds", () => {
  render(
    <RaceConditionAnalysisSection
      afterTargetRaces={null}
      finishPositionStats={[
        {
          averageOdds: 3.2,
          averagePopularity: 2.5,
          count: 4,
          details: [],
          finishPosition: 1,
          medianOdds: 3,
          medianPopularity: 2,
        },
        {
          averageOdds: 8.1,
          averagePopularity: 6.4,
          count: 4,
          details: [],
          finishPosition: 4,
          medianOdds: 7.5,
          medianPopularity: 6,
        },
      ]}
      payoutStats={[]}
      raceTimeStats={raceTimeStats}
    />,
  );
  expect(screen.getByRole("figure", { name: "着順別人気オッズグラフ" })).toBeDefined();
  expect(screen.getByLabelText("着順別単勝オッズ箱ひげ図")).toBeDefined();
  expect(screen.getAllByText("1着").length).toBe(1);
  expect(screen.getAllByText("着外").length).toBe(1);
  expect(document.querySelectorAll('[data-chart-point="finish-bee"]').length).toBe(2);
  expect(screen.queryByText("着順別のグラフに使えるデータがありません。")).toBeNull();
});

it("restores the finish chart preference from the current user", async () => {
  vi.mocked(loadConditionFinishChartForCurrentUser).mockResolvedValue(false);
  render(
    <RaceConditionAnalysisSection
      afterTargetRaces={null}
      finishPositionStats={finishPositionStats}
      payoutStats={[]}
      raceTimeStats={raceTimeStats}
    />,
  );
  await waitFor(() => {
    expect(screen.getByRole("columnheader", { name: "着順" })).toBeDefined();
  });
});
