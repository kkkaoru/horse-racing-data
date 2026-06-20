// Run with: bunx vitest run src/app/races/detail/paddock-recent-results-chart.test.tsx

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import React from "react";
import type { ReactElement, ReactNode } from "react";
import { afterEach, expect, test, vi } from "vitest";

import type { HorseRaceResult } from "../../../lib/race-types";
import {
  PaddockChartDot,
  PaddockRecentResultsChart,
  PaddockRecentTooltip,
  shouldRenderBlinkerRing,
} from "./paddock-recent-results-chart";

interface ChartChildrenStubProps {
  children?: ReactNode;
}

interface ComposedChartRowStub {
  blinker: string | null;
  dateValue: number;
  futan: number | null;
  isUpcoming?: boolean;
  popularity: number | null;
  weight: number | null;
  weightDelta: number | null;
}

interface ComposedChartStubProps {
  children?: ReactNode;
  data?: ComposedChartRowStub[];
}

interface LineStubProps {
  dataKey?: string;
  name?: string;
  stroke?: string;
  strokeDasharray?: string;
  yAxisId?: string;
}

interface YAxisStubProps {
  label?: string;
  reversed?: boolean;
  tickCount?: number;
  width?: number;
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

interface CartesianGridStubProps {
  stroke?: string;
  strokeDasharray?: string;
}

interface PaddockTooltipInjectedProps {
  active?: boolean;
  payload?: { payload: HorseRaceResultRowStub }[];
}

interface HorseRaceResultRowStub {
  blinker: string | null;
  dateValue: number;
  finish: number | null;
  futan: number | null;
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
  blinker: "1",
  dateValue: 0,
  finish: 1,
  futan: 55,
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
  CartesianGrid: ({ stroke, strokeDasharray }: CartesianGridStubProps) => (
    <div data-dash={strokeDasharray} data-stroke={stroke} data-testid="cartesian-grid-stub" />
  ),
  ComposedChart: ({ children, data }: ComposedChartStubProps) => (
    <div
      data-first-blinker={data?.at(0)?.blinker ?? "none"}
      data-last-blinker={data?.at(-1)?.blinker ?? "none"}
      data-last-futan={String(data?.at(-1)?.futan ?? "none")}
      data-last-is-upcoming={String(data?.at(-1)?.isUpcoming ?? false)}
      data-last-popularity={String(data?.at(-1)?.popularity ?? "none")}
      data-last-weight={String(data?.at(-1)?.weight ?? "none")}
      data-last-weight-delta={String(data?.at(-1)?.weightDelta ?? "none")}
      data-row-count={data?.length ?? 0}
      data-testid="composed-chart-stub"
    >
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
  YAxis: ({ label, reversed, tickCount, width, yAxisId }: YAxisStubProps) => (
    <div
      data-label={label}
      data-testid="y-axis-stub"
      data-tick-count={String(tickCount)}
      data-width={String(width)}
      data-y-axis-id={yAxisId}
    >
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

test("renders finish, popularity, weight and weightDelta lines by default and hides futan", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(screen.getAllByTestId("composed-chart-stub").length).toStrictEqual(1);
  expect(screen.getAllByTestId("line-stub").map((line) => line.textContent)).toStrictEqual([
    "着順",
    "人気",
    "馬体重",
    "馬体重増減",
  ]);
});

test("pressing the futan chip adds the violet futan line on the futan axis", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  fireEvent.click(screen.getByRole("button", { name: "斤量" }));
  expect(screen.getAllByTestId("line-stub").at(4)?.getAttribute("data-data-key")).toStrictEqual(
    "futan",
  );
  expect(screen.getAllByTestId("line-stub").at(4)?.getAttribute("data-stroke")).toStrictEqual(
    "#7048e8",
  );
  expect(screen.getAllByTestId("line-stub").at(4)?.getAttribute("data-y-axis-id")).toStrictEqual(
    "futan",
  );
  expect(screen.getAllByTestId("line-stub").at(4)?.getAttribute("data-dash")).toStrictEqual("4 4");
});

test("renders the futan chip pressed off by default", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(screen.getByRole("button", { name: "斤量" }).getAttribute("aria-pressed")).toStrictEqual(
    "false",
  );
});

test("the futan chip exposes the shared chip class and data-active flag", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(screen.getByRole("button", { name: "斤量" }).getAttribute("class")).toStrictEqual(
    "stats-control-button race-results-chart-chip",
  );
  expect(screen.getByRole("button", { name: "斤量" }).getAttribute("data-active")).toStrictEqual(
    "false",
  );
});

test("an active chip reports data-active true while staying aria-pressed", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(screen.getByRole("button", { name: "着順" }).getAttribute("data-active")).toStrictEqual(
    "true",
  );
  expect(screen.getByRole("button", { name: "着順" }).getAttribute("aria-pressed")).toStrictEqual(
    "true",
  );
});

test("renders the weight-delta series as an amber line on the delta axis", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(screen.getAllByTestId("line-stub").at(3)?.getAttribute("data-data-key")).toStrictEqual(
    "weightDelta",
  );
  expect(screen.getAllByTestId("line-stub").at(3)?.getAttribute("data-stroke")).toStrictEqual(
    "#f59f00",
  );
  expect(screen.getAllByTestId("line-stub").at(3)?.getAttribute("data-y-axis-id")).toStrictEqual(
    "delta",
  );
});

test("maps each visible metric line to its data key and revised stroke color", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-data-key")),
  ).toStrictEqual(["finish", "popularity", "weight", "weightDelta"]);
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-stroke")),
  ).toStrictEqual(["#e03131", "#1971c2", "#0ca678", "#f59f00"]);
});

test("renders the finish line solid and the popularity line dashed", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-dash")),
  ).toStrictEqual(["none", "6 3", "none", "none"]);
});

test("assigns the rank, weight and delta axes to their default metric lines", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-y-axis-id")),
  ).toStrictEqual(["rank", "rank", "weight", "delta"]);
});

test("renders three visible y axes in rank, weight, delta order by default", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(
    screen.getAllByTestId("y-axis-stub").map((axis) => axis.getAttribute("data-y-axis-id")),
  ).toStrictEqual(["rank", "weight", "delta"]);
});

test("shows the futan axis only after the futan chip is pressed", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  fireEvent.click(screen.getByRole("button", { name: "斤量" }));
  expect(
    screen.getAllByTestId("y-axis-stub").map((axis) => axis.getAttribute("data-y-axis-id")),
  ).toStrictEqual(["rank", "weight", "delta", "futan"]);
});

test("reverses only the rank axis and keeps the weight and delta axes normal", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(screen.getAllByTestId("y-axis-stub").map((axis) => axis.textContent)).toStrictEqual([
    "reversed",
    "normal",
    "normal",
  ]);
});

test("labels the three default y axes with their metric names", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(
    screen.getAllByTestId("y-axis-stub").map((axis) => axis.getAttribute("data-label")),
  ).toStrictEqual(["着順/人気", "馬体重", "増減"]);
});

test("labels the futan axis with its metric name once shown", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  fireEvent.click(screen.getByRole("button", { name: "斤量" }));
  expect(
    screen.getAllByTestId("y-axis-stub").map((axis) => axis.getAttribute("data-label")),
  ).toStrictEqual(["着順/人気", "馬体重", "増減", "斤量"]);
});

test("widens the value axes and caps tick counts for readability", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(
    screen.getAllByTestId("y-axis-stub").map((axis) => axis.getAttribute("data-width")),
  ).toStrictEqual(["36", "48", "44"]);
  expect(
    screen.getAllByTestId("y-axis-stub").map((axis) => axis.getAttribute("data-tick-count")),
  ).toStrictEqual(["6", "6", "6"]);
});

test("renders a dashed cartesian grid behind the chart", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(screen.getByTestId("cartesian-grid-stub").getAttribute("data-dash")).toStrictEqual("3 3");
  expect(screen.getByTestId("cartesian-grid-stub").getAttribute("data-stroke")).toStrictEqual(
    "#e9ecef",
  );
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
    "#adb5bd",
  );
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

test("renders four metric chips pressed on and the futan chip pressed off by default", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(screen.getByRole("button", { name: "着順" }).getAttribute("aria-pressed")).toStrictEqual(
    "true",
  );
  expect(screen.getByRole("button", { name: "人気" }).getAttribute("aria-pressed")).toStrictEqual(
    "true",
  );
  expect(screen.getByRole("button", { name: "馬体重" }).getAttribute("aria-pressed")).toStrictEqual(
    "true",
  );
  expect(
    screen.getByRole("button", { name: "馬体重増減" }).getAttribute("aria-pressed"),
  ).toStrictEqual("true");
  expect(screen.getByRole("button", { name: "斤量" }).getAttribute("aria-pressed")).toStrictEqual(
    "false",
  );
});

test("hiding the weight chip drops the weight line and hides the weight axis", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  fireEvent.click(screen.getByRole("button", { name: "馬体重" }));
  expect(screen.getByRole("button", { name: "馬体重" }).getAttribute("aria-pressed")).toStrictEqual(
    "false",
  );
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-data-key")),
  ).toStrictEqual(["finish", "popularity", "weightDelta"]);
  expect(
    screen.getAllByTestId("y-axis-stub").map((axis) => axis.getAttribute("data-y-axis-id")),
  ).toStrictEqual(["rank", "delta"]);
});

test("hiding the delta chip drops the delta line, axis and the zero reference line", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  fireEvent.click(screen.getByRole("button", { name: "馬体重増減" }));
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-data-key")),
  ).toStrictEqual(["finish", "popularity", "weight"]);
  expect(
    screen.getAllByTestId("y-axis-stub").map((axis) => axis.getAttribute("data-y-axis-id")),
  ).toStrictEqual(["rank", "weight"]);
  expect(screen.queryAllByTestId("reference-line-stub").length).toStrictEqual(0);
});

test("hiding only finish keeps the rank axis because popularity still uses it", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  fireEvent.click(screen.getByRole("button", { name: "着順" }));
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-data-key")),
  ).toStrictEqual(["popularity", "weight", "weightDelta"]);
  expect(
    screen.getAllByTestId("y-axis-stub").map((axis) => axis.getAttribute("data-y-axis-id")),
  ).toStrictEqual(["rank", "weight", "delta"]);
});

test("hiding both finish and popularity removes the rank axis and both rank lines", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  fireEvent.click(screen.getByRole("button", { name: "着順" }));
  fireEvent.click(screen.getByRole("button", { name: "人気" }));
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-data-key")),
  ).toStrictEqual(["weight", "weightDelta"]);
  expect(
    screen.getAllByTestId("y-axis-stub").map((axis) => axis.getAttribute("data-y-axis-id")),
  ).toStrictEqual(["weight", "delta"]);
});

test("re-pressing a hidden chip restores its line and axis", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  fireEvent.click(screen.getByRole("button", { name: "馬体重" }));
  fireEvent.click(screen.getByRole("button", { name: "馬体重" }));
  expect(screen.getByRole("button", { name: "馬体重" }).getAttribute("aria-pressed")).toStrictEqual(
    "true",
  );
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-data-key")),
  ).toStrictEqual(["finish", "popularity", "weight", "weightDelta"]);
});

test("the combine toggle is pressed off by default and labels the weight axis as plain weight", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(
    screen.getByRole("button", { name: "馬体重に斤量を合算" }).getAttribute("aria-pressed"),
  ).toStrictEqual("false");
  expect(
    screen.getAllByTestId("y-axis-stub").map((axis) => axis.getAttribute("data-label")),
  ).toStrictEqual(["着順/人気", "馬体重", "増減"]);
});

test("turning the combine toggle on sums carried weight into the weight row value", () => {
  render(
    <PaddockRecentResultsChart results={[chartResult({ bataiju: "480", futanJuryo: "550" })]} />,
  );
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-last-weight")).toStrictEqual(
    "480",
  );
  fireEvent.click(screen.getByRole("button", { name: "馬体重に斤量を合算" }));
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-last-weight")).toStrictEqual(
    "535",
  );
});

test("turning the combine toggle on relabels the weight axis and chip", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  fireEvent.click(screen.getByRole("button", { name: "馬体重に斤量を合算" }));
  expect(
    screen.getAllByTestId("y-axis-stub").map((axis) => axis.getAttribute("data-label")),
  ).toStrictEqual(["着順/人気", "馬体重+斤量", "増減"]);
  expect(
    screen.getByRole("button", { name: "馬体重+斤量" }).getAttribute("aria-pressed"),
  ).toStrictEqual("true");
});

test("the period slider defaults to 全N走 and the most recent ten races when fewer than ten exist", () => {
  render(
    <PaddockRecentResultsChart
      results={[
        chartResult({ kaisaiTsukihi: "0101", raceBango: "01" }),
        chartResult({ kaisaiTsukihi: "0202", raceBango: "02" }),
        chartResult({ kaisaiTsukihi: "0303", raceBango: "03" }),
      ]}
    />,
  );
  expect(screen.getByText("全3走").textContent).toStrictEqual("全3走");
  expect(screen.getByRole("slider").getAttribute("value")).toStrictEqual("3");
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-row-count")).toStrictEqual(
    "3",
  );
});

test("the period slider defaults to 直近10走 when more than ten races exist", () => {
  render(
    <PaddockRecentResultsChart
      results={[
        chartResult({ kaisaiTsukihi: "0101", raceBango: "01" }),
        chartResult({ kaisaiTsukihi: "0102", raceBango: "02" }),
        chartResult({ kaisaiTsukihi: "0103", raceBango: "03" }),
        chartResult({ kaisaiTsukihi: "0104", raceBango: "04" }),
        chartResult({ kaisaiTsukihi: "0105", raceBango: "05" }),
        chartResult({ kaisaiTsukihi: "0106", raceBango: "06" }),
        chartResult({ kaisaiTsukihi: "0107", raceBango: "07" }),
        chartResult({ kaisaiTsukihi: "0108", raceBango: "08" }),
        chartResult({ kaisaiTsukihi: "0109", raceBango: "09" }),
        chartResult({ kaisaiTsukihi: "0110", raceBango: "10" }),
        chartResult({ kaisaiTsukihi: "0111", raceBango: "11" }),
        chartResult({ kaisaiTsukihi: "0112", raceBango: "12" }),
      ]}
    />,
  );
  expect(screen.getByText("直近10走").textContent).toStrictEqual("直近10走");
  expect(screen.getByRole("slider").getAttribute("value")).toStrictEqual("10");
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-row-count")).toStrictEqual(
    "10",
  );
});

test("lowering the period slider trims the chart to the most recent races and shows 直近N走", () => {
  render(
    <PaddockRecentResultsChart
      results={[
        chartResult({ kaisaiTsukihi: "0101", raceBango: "01" }),
        chartResult({ kaisaiTsukihi: "0202", raceBango: "02" }),
        chartResult({ kaisaiTsukihi: "0303", raceBango: "03" }),
        chartResult({ kaisaiTsukihi: "0404", raceBango: "04" }),
        chartResult({ kaisaiTsukihi: "0505", raceBango: "05" }),
      ]}
    />,
  );
  expect(screen.getByText("全5走").textContent).toStrictEqual("全5走");
  fireEvent.change(screen.getByRole("slider"), { target: { value: "3" } });
  expect(screen.getByText("直近3走").textContent).toStrictEqual("直近3走");
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-row-count")).toStrictEqual(
    "3",
  );
});

test("orders same-date races by raceBango ascending via the tie-breaker comparator", () => {
  render(
    <PaddockRecentResultsChart
      results={[
        chartResult({ kaisaiTsukihi: "0322", raceBango: "02", bataiju: "470" }),
        chartResult({ kaisaiTsukihi: "0322", raceBango: "01", bataiju: "460" }),
      ]}
    />,
  );
  expect(screen.getByText("全2走").textContent).toStrictEqual("全2走");
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-row-count")).toStrictEqual(
    "2",
  );
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-last-weight")).toStrictEqual(
    "470",
  );
});

test("keeps the upcoming point even when it falls outside a trimmed recent window", () => {
  render(
    <PaddockRecentResultsChart
      results={[
        chartResult({ kaisaiTsukihi: "0101", raceBango: "01" }),
        chartResult({ kaisaiTsukihi: "0202", raceBango: "02" }),
        chartResult({ kaisaiTsukihi: "0303", raceBango: "03" }),
        chartResult({ kaisaiTsukihi: "0404", raceBango: "04" }),
      ]}
      upcomingRaceDate="20260614"
      upcomingWeight={500}
      upcomingWeightDelta={2}
    />,
  );
  fireEvent.change(screen.getByRole("slider"), { target: { value: "3" } });
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-row-count")).toStrictEqual(
    "4",
  );
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-is-upcoming"),
  ).toStrictEqual("true");
});

test("appends the upcoming weight point as the newest row when props are valid", () => {
  render(
    <PaddockRecentResultsChart
      results={[chartResult({ kaisaiTsukihi: "0322" })]}
      upcomingRaceDate="20260614"
      upcomingWeight={486}
      upcomingWeightDelta={6}
    />,
  );
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-row-count")).toStrictEqual(
    "2",
  );
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-is-upcoming"),
  ).toStrictEqual("true");
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-last-weight")).toStrictEqual(
    "486",
  );
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-weight-delta"),
  ).toStrictEqual("6");
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-last-futan")).toStrictEqual(
    "none",
  );
});

test("falls back to a null upcoming delta when the delta prop is not finite", () => {
  render(
    <PaddockRecentResultsChart
      results={[chartResult({ kaisaiTsukihi: "0322" })]}
      upcomingRaceDate="20260614"
      upcomingWeight={486}
      upcomingWeightDelta={Number.NaN}
    />,
  );
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-is-upcoming"),
  ).toStrictEqual("true");
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-weight-delta"),
  ).toStrictEqual("none");
});

test("omits the upcoming point when the race date is not eight digits", () => {
  render(
    <PaddockRecentResultsChart
      results={[chartResult({ kaisaiTsukihi: "0322" })]}
      upcomingRaceDate="2026614"
      upcomingWeight={486}
      upcomingWeightDelta={6}
    />,
  );
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-row-count")).toStrictEqual(
    "1",
  );
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-is-upcoming"),
  ).toStrictEqual("false");
});

test("still emits the upcoming row from a finite delta when the weight prop is not finite", () => {
  render(
    <PaddockRecentResultsChart
      results={[chartResult({ kaisaiTsukihi: "0322" })]}
      upcomingRaceDate="20260614"
      upcomingWeight={null}
      upcomingWeightDelta={6}
    />,
  );
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-row-count")).toStrictEqual(
    "2",
  );
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-is-upcoming"),
  ).toStrictEqual("true");
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-last-weight")).toStrictEqual(
    "none",
  );
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-weight-delta"),
  ).toStrictEqual("6");
});

test("omits the upcoming row when weight, delta and popularity are all absent", () => {
  render(
    <PaddockRecentResultsChart
      results={[chartResult({ kaisaiTsukihi: "0322" })]}
      upcomingRaceDate="20260614"
      upcomingWeight={null}
      upcomingWeightDelta={null}
    />,
  );
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-row-count")).toStrictEqual(
    "1",
  );
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-is-upcoming"),
  ).toStrictEqual("false");
});

test("plots the upcoming popularity even when no weight snapshot exists yet", () => {
  render(
    <PaddockRecentResultsChart
      results={[chartResult({ kaisaiTsukihi: "0322" })]}
      upcomingPopularity={2}
      upcomingRaceDate="20260614"
      upcomingWeight={null}
      upcomingWeightDelta={null}
    />,
  );
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-row-count")).toStrictEqual(
    "2",
  );
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-is-upcoming"),
  ).toStrictEqual("true");
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-popularity"),
  ).toStrictEqual("2");
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-last-weight")).toStrictEqual(
    "none",
  );
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-weight-delta"),
  ).toStrictEqual("none");
});

test("plots the upcoming weight with an absent popularity when only weight is provided", () => {
  render(
    <PaddockRecentResultsChart
      results={[chartResult({ kaisaiTsukihi: "0322" })]}
      upcomingRaceDate="20260614"
      upcomingWeight={486}
      upcomingWeightDelta={6}
    />,
  );
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-is-upcoming"),
  ).toStrictEqual("true");
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-last-weight")).toStrictEqual(
    "486",
  );
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-popularity"),
  ).toStrictEqual("none");
});

test("plots both upcoming weight and popularity when both are provided", () => {
  render(
    <PaddockRecentResultsChart
      results={[chartResult({ kaisaiTsukihi: "0322" })]}
      upcomingPopularity={3}
      upcomingRaceDate="20260614"
      upcomingWeight={500}
      upcomingWeightDelta={4}
    />,
  );
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-last-weight")).toStrictEqual(
    "500",
  );
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-weight-delta"),
  ).toStrictEqual("4");
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-popularity"),
  ).toStrictEqual("3");
});

test("PaddockChartDot renders a larger radius four circle for the upcoming point", () => {
  const { container } = render(
    <svg>
      <PaddockChartDot cx={10} cy={20} payload={{ isUpcoming: true }} stroke="#0ca678" />
    </svg>,
  );
  const circle = container.querySelector("circle");
  expect(circle?.getAttribute("r")).toStrictEqual("4");
  expect(circle?.getAttribute("fill")).toStrictEqual("#0ca678");
});

test("PaddockChartDot renders the default radius two circle for a past point", () => {
  const { container } = render(
    <svg>
      <PaddockChartDot cx={10} cy={20} payload={{ isUpcoming: false }} stroke="#e03131" />
    </svg>,
  );
  const circle = container.querySelector("circle");
  expect(circle?.getAttribute("r")).toStrictEqual("2");
  expect(circle?.getAttribute("stroke")).toStrictEqual("#e03131");
});

test("PaddockChartDot renders nothing when the coordinates are missing", () => {
  const { container } = render(
    <svg>
      <PaddockChartDot payload={{ isUpcoming: true }} stroke="#0ca678" />
    </svg>,
  );
  expect(container.querySelectorAll("circle").length).toStrictEqual(0);
});

test("PaddockChartDot renders nothing when only the y coordinate is missing", () => {
  const { container } = render(
    <svg>
      <PaddockChartDot cx={10} payload={{ isUpcoming: false }} stroke="#e03131" />
    </svg>,
  );
  expect(container.querySelectorAll("circle").length).toStrictEqual(0);
});

test("PaddockChartDot draws the blinker ring plus a normal dot for a worn past point", () => {
  const { container } = render(
    <svg>
      <PaddockChartDot
        cx={10}
        cy={20}
        payload={{ blinker: "1", isUpcoming: false }}
        stroke="#0ca678"
      />
    </svg>,
  );
  const circles = container.querySelectorAll("circle");
  expect(circles.length).toStrictEqual(2);
  expect(circles[0]?.getAttribute("r")).toStrictEqual("5");
  expect(circles[0]?.getAttribute("fill")).toStrictEqual("none");
  expect(circles[0]?.getAttribute("stroke")).toStrictEqual("#0ca678");
  expect(circles[1]?.getAttribute("r")).toStrictEqual("2");
  expect(circles[1]?.getAttribute("fill")).toStrictEqual("#0ca678");
});

test("PaddockChartDot draws only the normal dot for a not-worn past point", () => {
  const { container } = render(
    <svg>
      <PaddockChartDot
        cx={10}
        cy={20}
        payload={{ blinker: "0", isUpcoming: false }}
        stroke="#e03131"
      />
    </svg>,
  );
  const circles = container.querySelectorAll("circle");
  expect(circles.length).toStrictEqual(1);
  expect(circles[0]?.getAttribute("r")).toStrictEqual("2");
  expect(circles[0]?.getAttribute("fill")).toStrictEqual("#e03131");
});

test("PaddockChartDot draws only the normal dot when the blinker flag is null", () => {
  const { container } = render(
    <svg>
      <PaddockChartDot
        cx={10}
        cy={20}
        payload={{ blinker: null, isUpcoming: false }}
        stroke="#1971c2"
      />
    </svg>,
  );
  const circles = container.querySelectorAll("circle");
  expect(circles.length).toStrictEqual(1);
  expect(circles[0]?.getAttribute("r")).toStrictEqual("2");
});

test("PaddockChartDot draws the wider ring around the larger dot for a worn upcoming point", () => {
  const { container } = render(
    <svg>
      <PaddockChartDot
        cx={10}
        cy={20}
        payload={{ blinker: "1", isUpcoming: true }}
        stroke="#0ca678"
      />
    </svg>,
  );
  const circles = container.querySelectorAll("circle");
  expect(circles.length).toStrictEqual(2);
  expect(circles[0]?.getAttribute("r")).toStrictEqual("7");
  expect(circles[0]?.getAttribute("fill")).toStrictEqual("none");
  expect(circles[0]?.getAttribute("stroke")).toStrictEqual("#0ca678");
  expect(circles[1]?.getAttribute("r")).toStrictEqual("4");
  expect(circles[1]?.getAttribute("fill")).toStrictEqual("#0ca678");
});

test("PaddockChartDot draws only the larger dot for a not-worn upcoming point", () => {
  const { container } = render(
    <svg>
      <PaddockChartDot
        cx={10}
        cy={20}
        payload={{ blinker: "0", isUpcoming: true }}
        stroke="#1971c2"
      />
    </svg>,
  );
  const circles = container.querySelectorAll("circle");
  expect(circles.length).toStrictEqual(1);
  expect(circles[0]?.getAttribute("r")).toStrictEqual("4");
  expect(circles[0]?.getAttribute("fill")).toStrictEqual("#1971c2");
});

test("shouldRenderBlinkerRing returns true for a worn past point", () => {
  expect(shouldRenderBlinkerRing({ blinker: "1", isUpcoming: false })).toStrictEqual(true);
});

test("shouldRenderBlinkerRing returns false for a not-worn past point", () => {
  expect(shouldRenderBlinkerRing({ blinker: "0", isUpcoming: false })).toStrictEqual(false);
});

test("shouldRenderBlinkerRing returns false for a null blinker flag", () => {
  expect(shouldRenderBlinkerRing({ blinker: null, isUpcoming: false })).toStrictEqual(false);
});

test("shouldRenderBlinkerRing returns false for an undefined blinker flag", () => {
  expect(shouldRenderBlinkerRing({ blinker: undefined, isUpcoming: undefined })).toStrictEqual(
    false,
  );
});

test("shouldRenderBlinkerRing returns true for the worn upcoming point", () => {
  expect(shouldRenderBlinkerRing({ blinker: "1", isUpcoming: true })).toStrictEqual(true);
});

test("shouldRenderBlinkerRing returns false for a not-worn upcoming point", () => {
  expect(shouldRenderBlinkerRing({ blinker: "0", isUpcoming: true })).toStrictEqual(false);
});

test("renders the on-chart blinker ring hint above the paddock chart", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(screen.getByText("○ = ブリンカー装着").textContent).toStrictEqual("○ = ブリンカー装着");
});

test("drops only the weight value when the weight prop is an infinite number", () => {
  render(
    <PaddockRecentResultsChart
      results={[chartResult({ kaisaiTsukihi: "0322" })]}
      upcomingRaceDate="20260614"
      upcomingWeight={Number.POSITIVE_INFINITY}
      upcomingWeightDelta={6}
    />,
  );
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-row-count")).toStrictEqual(
    "2",
  );
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-is-upcoming"),
  ).toStrictEqual("true");
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-last-weight")).toStrictEqual(
    "none",
  );
});

test("omits the upcoming row when the date is absent even though popularity is finite", () => {
  render(
    <PaddockRecentResultsChart
      results={[chartResult({ kaisaiTsukihi: "0322" })]}
      upcomingPopularity={2}
      upcomingRaceDate={null}
      upcomingWeight={null}
      upcomingWeightDelta={null}
    />,
  );
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-row-count")).toStrictEqual(
    "1",
  );
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-is-upcoming"),
  ).toStrictEqual("false");
});

test("renders the tooltip with the date, every metric value and the metadata fields", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(screen.getByText("1970/01/01").textContent).toStrictEqual("1970/01/01");
  expect(screen.getByText("着順 1着").textContent).toStrictEqual("着順 1着");
  expect(screen.getByText("人気 3番人気").textContent).toStrictEqual("人気 3番人気");
  expect(screen.getByText("馬体重 480kg").textContent).toStrictEqual("馬体重 480kg");
  expect(screen.getByText("馬体重増減 6kg").textContent).toStrictEqual("馬体重増減 6kg");
  expect(screen.getByText("斤量 55kg").textContent).toStrictEqual("斤量 55kg");
  expect(screen.getByText("枠番 3").textContent).toStrictEqual("枠番 3");
  expect(screen.getByText("騎手 ルメール").textContent).toStrictEqual("騎手 ルメール");
  expect(screen.getByText("距離 2000m").textContent).toStrictEqual("距離 2000m");
  expect(screen.getByText("競馬場 東京").textContent).toStrictEqual("競馬場 東京");
  expect(screen.getByText("ブリンカー ○").textContent).toStrictEqual("ブリンカー ○");
});

test("renders the tooltip with a white background and dark text for mobile readability", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  const tooltip = screen.getByText("1970/01/01").parentElement;
  expect(tooltip?.className).toStrictEqual("race-results-chart-tooltip");
  expect(tooltip?.style.backgroundColor).toStrictEqual("#ffffff");
  expect(tooltip?.style.color).toStrictEqual("#1f2937");
  expect(tooltip?.style.maxWidth).toStrictEqual("220px");
});

test("PaddockRecentTooltip renders nothing when it is not active", () => {
  const { container } = render(
    <PaddockRecentTooltip
      active={false}
      payload={[
        {
          payload: {
            blinker: "1",
            dateValue: 0,
            finish: 1,
            futan: 55,
            isUpcoming: false,
            keibajoCode: "05",
            kishumeiRyakusho: "ルメール",
            kyori: "2000",
            popularity: 3,
            raceDate: "20260322",
            wakuban: "3",
            weight: 480,
            weightDelta: 6,
          },
        },
      ]}
    />,
  );
  expect(container.querySelectorAll("div").length).toStrictEqual(0);
});

test("PaddockRecentTooltip renders nothing when there is no payload entry", () => {
  const { container } = render(<PaddockRecentTooltip active={true} payload={[]} />);
  expect(container.querySelectorAll("div").length).toStrictEqual(0);
});

test("PaddockRecentTooltip omits null metric lines and shows dash meta with a fallback frame", () => {
  render(
    <PaddockRecentTooltip
      active={true}
      payload={[
        {
          payload: {
            blinker: null,
            dateValue: 0,
            finish: null,
            futan: null,
            isUpcoming: false,
            keibajoCode: "",
            kishumeiRyakusho: null,
            kyori: null,
            popularity: null,
            raceDate: "20260322",
            wakuban: null,
            weight: null,
            weightDelta: null,
          },
        },
      ]}
    />,
  );
  expect(screen.queryAllByText("着順 1着").length).toStrictEqual(0);
  expect(screen.getByText("枠番 -").textContent).toStrictEqual("枠番 -");
  expect(screen.getByText("騎手 -").textContent).toStrictEqual("騎手 -");
  const tooltip = screen.getByText("1970/01/01").parentElement;
  expect(tooltip?.style.borderColor).toStrictEqual("#adb5bd");
});

test("PaddockRecentTooltip treats whitespace-only jockey and wakuban as a dash", () => {
  render(
    <PaddockRecentTooltip
      active={true}
      payload={[
        {
          payload: {
            blinker: null,
            dateValue: 0,
            finish: 2,
            futan: 55,
            isUpcoming: false,
            keibajoCode: "05",
            kishumeiRyakusho: "  ",
            kyori: "2000",
            popularity: 4,
            raceDate: "20260322",
            wakuban: "  ",
            weight: 480,
            weightDelta: -4,
          },
        },
      ]}
    />,
  );
  expect(screen.getByText("枠番 -").textContent).toStrictEqual("枠番 -");
  expect(screen.getByText("騎手 -").textContent).toStrictEqual("騎手 -");
});

test("populates the chart row blinker flag from the worn result", () => {
  render(<PaddockRecentResultsChart results={[chartResult({ blinkerShiyoKubun: "1" })]} />);
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-first-blinker"),
  ).toStrictEqual("1");
});

test("populates the chart row blinker flag from the not-worn result", () => {
  render(<PaddockRecentResultsChart results={[chartResult({ blinkerShiyoKubun: "0" })]} />);
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-first-blinker"),
  ).toStrictEqual("0");
});

test("leaves the chart row blinker flag none when the result has no blinker value", () => {
  render(<PaddockRecentResultsChart results={[chartResult({ blinkerShiyoKubun: null })]} />);
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-first-blinker"),
  ).toStrictEqual("none");
});

test("leaves the upcoming synthetic row blinker flag none when no upcoming blinker is given", () => {
  render(
    <PaddockRecentResultsChart
      results={[chartResult({ blinkerShiyoKubun: "1", kaisaiTsukihi: "0322" })]}
      upcomingRaceDate="20260614"
      upcomingWeight={486}
      upcomingWeightDelta={6}
    />,
  );
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-last-blinker")).toStrictEqual(
    "none",
  );
});

test("carries the worn upcoming blinker flag onto the synthetic row", () => {
  render(
    <PaddockRecentResultsChart
      results={[chartResult({ blinkerShiyoKubun: "0", kaisaiTsukihi: "0322" })]}
      upcomingBlinker="1"
      upcomingRaceDate="20260614"
      upcomingWeight={486}
      upcomingWeightDelta={6}
    />,
  );
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-last-blinker")).toStrictEqual(
    "1",
  );
});

test("treats a blank upcoming blinker flag as none on the synthetic row", () => {
  render(
    <PaddockRecentResultsChart
      results={[chartResult({ blinkerShiyoKubun: "0", kaisaiTsukihi: "0322" })]}
      upcomingBlinker=" "
      upcomingRaceDate="20260614"
      upcomingWeight={486}
      upcomingWeightDelta={6}
    />,
  );
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-last-blinker")).toStrictEqual(
    "none",
  );
});

test("PaddockRecentTooltip shows the worn blinker line when the row blinker is 1", () => {
  render(
    <PaddockRecentTooltip
      active={true}
      payload={[
        {
          payload: {
            blinker: "1",
            dateValue: 0,
            finish: 1,
            futan: 55,
            isUpcoming: false,
            keibajoCode: "05",
            kishumeiRyakusho: "ルメール",
            kyori: "2000",
            popularity: 3,
            raceDate: "20260322",
            wakuban: "3",
            weight: 480,
            weightDelta: 6,
          },
        },
      ]}
    />,
  );
  expect(screen.getByText("ブリンカー ○").textContent).toStrictEqual("ブリンカー ○");
});

test("PaddockRecentTooltip omits the blinker line when the row blinker is 0", () => {
  render(
    <PaddockRecentTooltip
      active={true}
      payload={[
        {
          payload: {
            blinker: "0",
            dateValue: 0,
            finish: 1,
            futan: 55,
            isUpcoming: false,
            keibajoCode: "05",
            kishumeiRyakusho: "ルメール",
            kyori: "2000",
            popularity: 3,
            raceDate: "20260322",
            wakuban: "3",
            weight: 480,
            weightDelta: 6,
          },
        },
      ]}
    />,
  );
  expect(screen.queryAllByText("ブリンカー ○").length).toStrictEqual(0);
});

test("renders the finish chip swatch with the crimson palette color", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  const swatch = screen.getByRole("button", { name: "着順" }).querySelector("span");
  expect(swatch?.style.backgroundColor).toStrictEqual("#e03131");
});

test("renders the popularity chip swatch with the blue palette color", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  const swatch = screen.getByRole("button", { name: "人気" }).querySelector("span");
  expect(swatch?.style.backgroundColor).toStrictEqual("#1971c2");
});

test("renders the weight chip swatch with the emerald palette color", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  const swatch = screen.getByRole("button", { name: "馬体重" }).querySelector("span");
  expect(swatch?.style.backgroundColor).toStrictEqual("#0ca678");
});

test("renders the weight-delta chip swatch with the amber palette color", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  const swatch = screen.getByRole("button", { name: "馬体重増減" }).querySelector("span");
  expect(swatch?.style.backgroundColor).toStrictEqual("#f59f00");
});

test("renders the futan chip swatch with the violet palette color", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  const swatch = screen.getByRole("button", { name: "斤量" }).querySelector("span");
  expect(swatch?.style.backgroundColor).toStrictEqual("#7048e8");
});

test("plots the upcoming popularity on the synthetic upcoming row when provided", () => {
  render(
    <PaddockRecentResultsChart
      results={[chartResult({ kaisaiTsukihi: "0322" })]}
      upcomingPopularity={2}
      upcomingRaceDate="20260614"
      upcomingWeight={486}
      upcomingWeightDelta={6}
    />,
  );
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-is-upcoming"),
  ).toStrictEqual("true");
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-popularity"),
  ).toStrictEqual("2");
});

test("omits the upcoming popularity when it is below the minimum rank of one", () => {
  render(
    <PaddockRecentResultsChart
      results={[chartResult({ kaisaiTsukihi: "0322" })]}
      upcomingPopularity={0}
      upcomingRaceDate="20260614"
      upcomingWeight={486}
      upcomingWeightDelta={6}
    />,
  );
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-is-upcoming"),
  ).toStrictEqual("true");
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-popularity"),
  ).toStrictEqual("none");
});

test("omits the upcoming popularity when it is not a finite number", () => {
  render(
    <PaddockRecentResultsChart
      results={[chartResult({ kaisaiTsukihi: "0322" })]}
      upcomingPopularity={Number.NaN}
      upcomingRaceDate="20260614"
      upcomingWeight={486}
      upcomingWeightDelta={6}
    />,
  );
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-popularity"),
  ).toStrictEqual("none");
});

test("omits the upcoming popularity when no upcoming popularity prop is given", () => {
  render(
    <PaddockRecentResultsChart
      results={[chartResult({ kaisaiTsukihi: "0322" })]}
      upcomingRaceDate="20260614"
      upcomingWeight={486}
      upcomingWeightDelta={6}
    />,
  );
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-is-upcoming"),
  ).toStrictEqual("true");
  expect(
    screen.getByTestId("composed-chart-stub").getAttribute("data-last-popularity"),
  ).toStrictEqual("none");
});

test("the period slider defaults to ten and exposes ten as the slider value when exactly ten races exist", () => {
  render(
    <PaddockRecentResultsChart
      results={[
        chartResult({ kaisaiTsukihi: "0101", raceBango: "01" }),
        chartResult({ kaisaiTsukihi: "0102", raceBango: "02" }),
        chartResult({ kaisaiTsukihi: "0103", raceBango: "03" }),
        chartResult({ kaisaiTsukihi: "0104", raceBango: "04" }),
        chartResult({ kaisaiTsukihi: "0105", raceBango: "05" }),
        chartResult({ kaisaiTsukihi: "0106", raceBango: "06" }),
        chartResult({ kaisaiTsukihi: "0107", raceBango: "07" }),
        chartResult({ kaisaiTsukihi: "0108", raceBango: "08" }),
        chartResult({ kaisaiTsukihi: "0109", raceBango: "09" }),
        chartResult({ kaisaiTsukihi: "0110", raceBango: "10" }),
      ]}
    />,
  );
  expect(screen.getByText("全10走").textContent).toStrictEqual("全10走");
  expect(screen.getByRole("slider").getAttribute("value")).toStrictEqual("10");
  expect(screen.getByRole("slider").getAttribute("max")).toStrictEqual("10");
});

test("the period slider defaults to all races when between three and ten races exist", () => {
  render(
    <PaddockRecentResultsChart
      results={[
        chartResult({ kaisaiTsukihi: "0101", raceBango: "01" }),
        chartResult({ kaisaiTsukihi: "0102", raceBango: "02" }),
        chartResult({ kaisaiTsukihi: "0103", raceBango: "03" }),
        chartResult({ kaisaiTsukihi: "0104", raceBango: "04" }),
        chartResult({ kaisaiTsukihi: "0105", raceBango: "05" }),
        chartResult({ kaisaiTsukihi: "0106", raceBango: "06" }),
        chartResult({ kaisaiTsukihi: "0107", raceBango: "07" }),
      ]}
    />,
  );
  expect(screen.getByText("全7走").textContent).toStrictEqual("全7走");
  expect(screen.getByRole("slider").getAttribute("value")).toStrictEqual("7");
  expect(screen.getByRole("slider").getAttribute("max")).toStrictEqual("7");
});

test("the period slider max equals the total so more than ten races stay selectable", () => {
  render(
    <PaddockRecentResultsChart
      results={[
        chartResult({ kaisaiTsukihi: "0101", raceBango: "01" }),
        chartResult({ kaisaiTsukihi: "0102", raceBango: "02" }),
        chartResult({ kaisaiTsukihi: "0103", raceBango: "03" }),
        chartResult({ kaisaiTsukihi: "0104", raceBango: "04" }),
        chartResult({ kaisaiTsukihi: "0105", raceBango: "05" }),
        chartResult({ kaisaiTsukihi: "0106", raceBango: "06" }),
        chartResult({ kaisaiTsukihi: "0107", raceBango: "07" }),
        chartResult({ kaisaiTsukihi: "0108", raceBango: "08" }),
        chartResult({ kaisaiTsukihi: "0109", raceBango: "09" }),
        chartResult({ kaisaiTsukihi: "0110", raceBango: "10" }),
        chartResult({ kaisaiTsukihi: "0111", raceBango: "11" }),
        chartResult({ kaisaiTsukihi: "0112", raceBango: "12" }),
      ]}
    />,
  );
  expect(screen.getByRole("slider").getAttribute("max")).toStrictEqual("12");
  fireEvent.change(screen.getByRole("slider"), { target: { value: "12" } });
  expect(screen.getByText("全12走").textContent).toStrictEqual("全12走");
  expect(screen.getByTestId("composed-chart-stub").getAttribute("data-row-count")).toStrictEqual(
    "12",
  );
});
