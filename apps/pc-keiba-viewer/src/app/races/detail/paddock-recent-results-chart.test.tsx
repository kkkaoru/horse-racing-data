// Run with: bunx vitest run src/app/races/detail/paddock-recent-results-chart.test.tsx

import { cleanup, render, screen } from "@testing-library/react";
import React from "react";
import type { ReactElement, ReactNode } from "react";
import { afterEach, expect, test, vi } from "vitest";

import type { HorseRaceResult } from "../../../lib/race-types";
import { PaddockRecentResultsChart } from "./paddock-recent-results-chart";

interface ChartChildrenStubProps {
  children?: ReactNode;
}

interface ComposedChartStubProps {
  children?: ReactNode;
  data?: { dateValue: number }[];
}

interface LineStubProps {
  dataKey?: string;
  name?: string;
  stroke?: string;
  strokeDasharray?: string;
  yAxisId?: string;
}

interface BarStubProps {
  children?: ReactNode;
  dataKey?: string;
  name?: string;
  yAxisId?: string;
}

interface CellStubProps {
  fill?: string;
}

interface YAxisStubProps {
  reversed?: boolean;
  yAxisId?: string;
}

interface ReferenceLineStubProps {
  stroke?: string;
  y?: number;
  yAxisId?: string;
}

interface XAxisStubProps {
  scale?: string;
}

interface PaddockTooltipInjectedProps {
  active?: boolean;
  payload?: { payload: HorseRaceResultRowStub }[];
}

interface HorseRaceResultRowStub {
  dateValue: number;
  finish: number | null;
  keibajoCode: string;
  kishumeiRyakusho: string | null;
  kyori: string | null;
  popularity: number | null;
  raceDate: string;
  wakuban: string | null;
  weight: number | null;
  weightDelta: number | null;
}

interface TooltipStubProps {
  content?: ReactElement<PaddockTooltipInjectedProps>;
}

const TOOLTIP_FIXTURE_ROW: HorseRaceResultRowStub = {
  dateValue: 0,
  finish: 1,
  keibajoCode: "05",
  kishumeiRyakusho: "ルメール",
  kyori: "2000",
  popularity: 3,
  raceDate: "20260322",
  wakuban: "3",
  weight: 480,
  weightDelta: 6,
};

vi.mock("recharts", () => ({
  Bar: ({ children, dataKey, name, yAxisId }: BarStubProps) => (
    <div data-data-key={dataKey} data-testid="bar-stub" data-y-axis-id={yAxisId}>
      {name}
      {children}
    </div>
  ),
  Cell: ({ fill }: CellStubProps) => <div data-fill={fill} data-testid="cell-stub" />,
  ComposedChart: ({ children, data }: ComposedChartStubProps) => (
    <div data-row-count={data?.length ?? 0} data-testid="composed-chart-stub">
      {children}
    </div>
  ),
  Legend: () => <div data-testid="legend-stub" />,
  Line: ({ dataKey, name, stroke, strokeDasharray, yAxisId }: LineStubProps) => (
    <div
      data-dash={strokeDasharray ?? "none"}
      data-data-key={dataKey}
      data-stroke={stroke}
      data-testid="line-stub"
      data-y-axis-id={yAxisId}
    >
      {name}
    </div>
  ),
  ReferenceLine: ({ stroke, y, yAxisId }: ReferenceLineStubProps) => (
    <div
      data-stroke={stroke}
      data-testid="reference-line-stub"
      data-y={String(y)}
      data-y-axis-id={yAxisId}
    />
  ),
  ResponsiveContainer: ({ children }: ChartChildrenStubProps) => (
    <div data-testid="responsive-container-stub">{children}</div>
  ),
  Tooltip: ({ content }: TooltipStubProps) => (
    <div data-testid="tooltip-stub">
      {content === undefined
        ? null
        : React.cloneElement(content, {
            active: true,
            payload: [{ payload: TOOLTIP_FIXTURE_ROW }],
          })}
    </div>
  ),
  XAxis: ({ scale }: XAxisStubProps) => <div data-scale={scale} data-testid="x-axis-stub" />,
  YAxis: ({ reversed, yAxisId }: YAxisStubProps) => (
    <div data-testid="y-axis-stub" data-y-axis-id={yAxisId}>
      {reversed === true ? "reversed" : "normal"}
    </div>
  ),
}));

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

const chartResult = (overrides: Partial<HorseRaceResult>): HorseRaceResult => ({
  babajotaiCodeDirt: "1",
  babajotaiCodeShiba: "0",
  bamei: "アルファ",
  banushimei: "馬主",
  barei: "04",
  bataiju: "480",
  chokyoshimeiRyakusho: "調教師",
  corner1: "03",
  corner2: "04",
  corner3: "05",
  corner4: "06",
  currentBarei: "04",
  currentJockey: "騎手",
  currentSeibetsuCode: "1",
  currentUmaban: "01",
  futanJuryo: "550",
  gradeCode: "00",
  hassoJikoku: "1200",
  juryoShubetsuCode: "1",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0322",
  kakuteiChakujun: "01",
  keibajoCode: "05",
  kettoTorokuBango: "2022100001",
  kishumeiRyakusho: "ルメール",
  kohan3f: "378",
  kyori: "1800",
  kyosoJokenCode: "005",
  kyosoJokenMeisho: "3歳",
  kyosoKigoCode: "000",
  kyosoShubetsuCode: "12",
  kyosomeiFukudai: null,
  kyosomeiHondai: "テストレース",
  kyosomeiKakkonai: null,
  raceBango: "01",
  seibetsuCode: "1",
  sohaTime: "1123",
  tanshoNinkijun: "02",
  tanshoOdds: "012",
  tenkoCode: "1",
  timeSa: null,
  trackCode: "24",
  umaban: "01",
  wakuban: "1",
  zogenFugo: "+",
  zogenSa: "002",
  ...overrides,
});

test("renders the empty state when no results are provided", () => {
  render(<PaddockRecentResultsChart results={[]} />);
  expect(screen.getByText("表示できるレースがありません").textContent).toStrictEqual(
    "表示できるレースがありません",
  );
  expect(screen.queryAllByTestId("composed-chart-stub").length).toStrictEqual(0);
});

test("renders the empty state when every row has an invalid race date", () => {
  render(
    <PaddockRecentResultsChart results={[chartResult({ kaisaiNen: "", kaisaiTsukihi: "0322" })]} />,
  );
  expect(screen.getByText("表示できるレースがありません").textContent).toStrictEqual(
    "表示できるレースがありません",
  );
  expect(screen.queryAllByTestId("composed-chart-stub").length).toStrictEqual(0);
});

test("renders all four metric series in one combined chart", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(screen.getAllByTestId("composed-chart-stub").length).toStrictEqual(1);
  expect(screen.getAllByTestId("line-stub").map((line) => line.textContent)).toStrictEqual([
    "馬体重",
    "着順",
    "人気",
  ]);
  expect(screen.getByTestId("bar-stub").getAttribute("data-data-key")).toStrictEqual("weightDelta");
});

test("maps each metric line to its data key and stroke color", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-data-key")),
  ).toStrictEqual(["weight", "finish", "popularity"]);
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-stroke")),
  ).toStrictEqual(["#0f766e", "#dc2626", "#2563eb"]);
});

test("renders the finish line solid and the popularity line dashed", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-dash")),
  ).toStrictEqual(["none", "none", "6 3"]);
});

test("assigns the rank axis to finish and popularity and the weight axis to weight", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-y-axis-id")),
  ).toStrictEqual(["weight", "rank", "rank"]);
});

test("reverses only the rank axis and keeps the weight and delta axes normal", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(screen.getAllByTestId("y-axis-stub").map((axis) => axis.textContent)).toStrictEqual([
    "reversed",
    "normal",
    "normal",
  ]);
  expect(
    screen.getAllByTestId("y-axis-stub").map((axis) => axis.getAttribute("data-y-axis-id")),
  ).toStrictEqual(["rank", "weight", "delta"]);
});

test("uses a time scale on the X axis", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(screen.getByTestId("x-axis-stub").getAttribute("data-scale")).toStrictEqual("time");
});

test("renders a zero reference line on the delta axis", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(screen.getByTestId("reference-line-stub").getAttribute("data-y")).toStrictEqual("0");
  expect(screen.getByTestId("reference-line-stub").getAttribute("data-y-axis-id")).toStrictEqual(
    "delta",
  );
  expect(screen.getByTestId("reference-line-stub").getAttribute("data-stroke")).toStrictEqual(
    "#9ca3af",
  );
});

test("colors the delta bars orange for non-negative deltas and blue for negative deltas", () => {
  render(
    <PaddockRecentResultsChart
      results={[
        chartResult({ kaisaiTsukihi: "0322", raceBango: "01", zogenFugo: "+", zogenSa: "006" }),
        chartResult({ kaisaiTsukihi: "0405", raceBango: "02", zogenFugo: "-", zogenSa: "004" }),
      ]}
    />,
  );
  expect(
    screen.getAllByTestId("cell-stub").map((cell) => cell.getAttribute("data-fill")),
  ).toStrictEqual(["#ea580c", "#2563eb"]);
});

test("renders the legend and passes every plottable row to the chart", () => {
  render(
    <PaddockRecentResultsChart
      results={[
        chartResult({ kaisaiTsukihi: "0322", raceBango: "01" }),
        chartResult({ kaisaiTsukihi: "0405", raceBango: "02" }),
      ]}
    />,
  );
  expect(screen.getByTestId("legend-stub").getAttribute("data-testid")).toStrictEqual(
    "legend-stub",
  );
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-row-count")).toStrictEqual(
    "2",
  );
});

test("renders the tooltip with the date, every metric value and the metadata fields", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(screen.getByText("1970/01/01").textContent).toStrictEqual("1970/01/01");
  expect(screen.getByText("着順 1着").textContent).toStrictEqual("着順 1着");
  expect(screen.getByText("人気 3番人気").textContent).toStrictEqual("人気 3番人気");
  expect(screen.getByText("馬体重 480kg").textContent).toStrictEqual("馬体重 480kg");
  expect(screen.getByText("馬体重増減 6kg").textContent).toStrictEqual("馬体重増減 6kg");
  expect(screen.getByText("枠番 3").textContent).toStrictEqual("枠番 3");
  expect(screen.getByText("騎手 ルメール").textContent).toStrictEqual("騎手 ルメール");
  expect(screen.getByText("距離 2000m").textContent).toStrictEqual("距離 2000m");
  expect(screen.getByText("競馬場 東京").textContent).toStrictEqual("競馬場 東京");
});
