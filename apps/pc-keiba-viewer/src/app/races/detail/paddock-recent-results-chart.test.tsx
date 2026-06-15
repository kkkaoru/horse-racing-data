// Run with: bunx vitest run src/app/races/detail/paddock-recent-results-chart.test.tsx

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import React from "react";
import type { ReactElement, ReactNode } from "react";
import { afterEach, expect, test, vi } from "vitest";

import type { HorseRaceResult } from "../../../lib/race-types";
import { PaddockChartDot, PaddockRecentResultsChart } from "./paddock-recent-results-chart";

interface ChartChildrenStubProps {
  children?: ReactNode;
}

interface ComposedChartRowStub {
  dateValue: number;
  isUpcoming?: boolean;
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
  CartesianGrid: ({ stroke, strokeDasharray }: CartesianGridStubProps) => (
    <div data-dash={strokeDasharray} data-stroke={stroke} data-testid="cartesian-grid-stub" />
  ),
  ComposedChart: ({ children, data }: ComposedChartStubProps) => (
    <div
      data-last-is-upcoming={String(data?.at(-1)?.isUpcoming ?? false)}
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
  YAxis: ({ label, reversed, yAxisId }: YAxisStubProps) => (
    <div data-label={label} data-testid="y-axis-stub" data-y-axis-id={yAxisId}>
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

test("renders all four metric series as lines in chip render order by default", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(screen.getAllByTestId("composed-chart-stub").length).toStrictEqual(1);
  expect(screen.getAllByTestId("line-stub").map((line) => line.textContent)).toStrictEqual([
    "着順",
    "人気",
    "馬体重",
    "馬体重増減",
  ]);
});

test("renders the weight-delta series as an orange line on the delta axis", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(screen.getAllByTestId("line-stub").at(3)?.getAttribute("data-data-key")).toStrictEqual(
    "weightDelta",
  );
  expect(screen.getAllByTestId("line-stub").at(3)?.getAttribute("data-stroke")).toStrictEqual(
    "#ea580c",
  );
  expect(screen.getAllByTestId("line-stub").at(3)?.getAttribute("data-y-axis-id")).toStrictEqual(
    "delta",
  );
});

test("maps each metric line to its data key and stroke color", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-data-key")),
  ).toStrictEqual(["finish", "popularity", "weight", "weightDelta"]);
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-stroke")),
  ).toStrictEqual(["#dc2626", "#2563eb", "#0f766e", "#ea580c"]);
});

test("renders the finish line solid and the popularity line dashed", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-dash")),
  ).toStrictEqual(["none", "6 3", "none", "none"]);
});

test("assigns the rank, weight and delta axes to their metric lines", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-y-axis-id")),
  ).toStrictEqual(["rank", "rank", "weight", "delta"]);
});

test("renders three visible y axes in rank, weight, delta order", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(
    screen.getAllByTestId("y-axis-stub").map((axis) => axis.getAttribute("data-y-axis-id")),
  ).toStrictEqual(["rank", "weight", "delta"]);
});

test("reverses only the rank axis and keeps the weight and delta axes normal", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(screen.getAllByTestId("y-axis-stub").map((axis) => axis.textContent)).toStrictEqual([
    "reversed",
    "normal",
    "normal",
  ]);
});

test("labels the three y axes with their metric names", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(
    screen.getAllByTestId("y-axis-stub").map((axis) => axis.getAttribute("data-label")),
  ).toStrictEqual(["着順/人気", "馬体重", "増減"]);
});

test("renders a dashed cartesian grid behind the chart", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  expect(screen.getByTestId("cartesian-grid-stub").getAttribute("data-dash")).toStrictEqual("3 3");
  expect(screen.getByTestId("cartesian-grid-stub").getAttribute("data-stroke")).toStrictEqual(
    "#e5e7eb",
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
    "#9ca3af",
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

test("renders all four toggle chips pressed on by default", () => {
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

test("omits the upcoming point when the weight prop is not a finite number", () => {
  render(
    <PaddockRecentResultsChart
      results={[chartResult({ kaisaiTsukihi: "0322" })]}
      upcomingRaceDate="20260614"
      upcomingWeight={null}
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

test("PaddockChartDot renders a larger radius four circle for the upcoming point", () => {
  const { container } = render(
    <svg>
      <PaddockChartDot cx={10} cy={20} payload={{ isUpcoming: true }} stroke="#0f766e" />
    </svg>,
  );
  const circle = container.querySelector("circle");
  expect(circle?.getAttribute("r")).toStrictEqual("4");
  expect(circle?.getAttribute("fill")).toStrictEqual("#0f766e");
});

test("PaddockChartDot renders the default radius two circle for a past point", () => {
  const { container } = render(
    <svg>
      <PaddockChartDot cx={10} cy={20} payload={{ isUpcoming: false }} stroke="#dc2626" />
    </svg>,
  );
  const circle = container.querySelector("circle");
  expect(circle?.getAttribute("r")).toStrictEqual("2");
  expect(circle?.getAttribute("stroke")).toStrictEqual("#dc2626");
});

test("PaddockChartDot renders nothing when the coordinates are missing", () => {
  const { container } = render(
    <svg>
      <PaddockChartDot payload={{ isUpcoming: true }} stroke="#0f766e" />
    </svg>,
  );
  expect(container.querySelectorAll("circle").length).toStrictEqual(0);
});

test("PaddockChartDot renders nothing when only the y coordinate is missing", () => {
  const { container } = render(
    <svg>
      <PaddockChartDot cx={10} payload={{ isUpcoming: false }} stroke="#dc2626" />
    </svg>,
  );
  expect(container.querySelectorAll("circle").length).toStrictEqual(0);
});

test("omits the upcoming point when the weight prop is an infinite number", () => {
  render(
    <PaddockRecentResultsChart
      results={[chartResult({ kaisaiTsukihi: "0322" })]}
      upcomingRaceDate="20260614"
      upcomingWeight={Number.POSITIVE_INFINITY}
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

test("renders the tooltip with a white background and dark text for mobile readability", () => {
  render(<PaddockRecentResultsChart results={[chartResult({})]} />);
  const tooltip = screen.getByText("1970/01/01").parentElement;
  expect(tooltip?.className).toStrictEqual("race-results-chart-tooltip");
  expect(tooltip?.style.backgroundColor).toStrictEqual("#ffffff");
  expect(tooltip?.style.color).toStrictEqual("#1f2937");
  expect(tooltip?.style.maxWidth).toStrictEqual("220px");
});
