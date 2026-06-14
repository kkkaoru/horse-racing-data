// Run with: bunx vitest run src/app/races/detail/horse-race-results-chart.test.tsx

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import React from "react";
import type { ReactElement, ReactNode } from "react";
import { afterEach, expect, test, vi } from "vitest";

import type { HorseRaceChartRunner } from "../../../lib/horse-race-results-chart-data";
import type { HorseWeightSnapshot } from "../../../lib/horse-weight-stream-client";
import { useHorseWeightStream } from "../../../lib/horse-weight-stream-client";
import type { HorseRaceResult } from "../../../lib/race-types";
import { HorseRaceResultsChart } from "./horse-race-results-chart";

interface ChartChildrenStubProps {
  children?: ReactNode;
}

interface LineChartStubProps {
  children?: ReactNode;
  syncId?: string;
}

interface LinePointStub {
  isUpcoming?: boolean;
}

interface LineStubProps {
  data?: LinePointStub[];
  name?: string;
  stroke?: string;
  strokeWidth?: number;
}

interface YAxisStubProps {
  reversed?: boolean;
}

interface MetricTooltipInjectedProps {
  active?: boolean;
  payload?: { payload: { dateValue: number; jockey: string; kyori: string }; value: number }[];
}

interface TooltipStubProps {
  content?: ReactElement<MetricTooltipInjectedProps>;
}

interface MatrixStubProps {
  rows: unknown[];
}

vi.mock("recharts", () => ({
  CartesianGrid: () => <div data-testid="cartesian-grid-stub" />,
  Line: ({ data, name, stroke, strokeWidth }: LineStubProps) => (
    <div
      data-stroke={stroke}
      data-stroke-width={strokeWidth}
      data-testid="line-stub"
      data-total-points={data?.length ?? 0}
      data-upcoming-points={data?.filter((point) => point.isUpcoming === true).length ?? 0}
    >
      {name}
    </div>
  ),
  LineChart: ({ children, syncId }: LineChartStubProps) => (
    <div data-sync-id={syncId} data-testid="line-chart-stub">
      {children}
    </div>
  ),
  ResponsiveContainer: ({ children }: ChartChildrenStubProps) => (
    <div data-testid="responsive-container-stub">{children}</div>
  ),
  Tooltip: ({ content }: TooltipStubProps) => (
    <div data-has-content={content === undefined ? "false" : "true"} data-testid="tooltip-stub">
      {content === undefined
        ? null
        : React.cloneElement(content, {
            active: true,
            payload: [{ payload: { dateValue: 0, jockey: "ルメール", kyori: "2000" }, value: 1 }],
          })}
    </div>
  ),
  XAxis: () => <div data-testid="x-axis-stub" />,
  YAxis: ({ reversed }: YAxisStubProps) => (
    <div data-testid="y-axis-stub">{reversed === true ? "reversed" : "normal"}</div>
  ),
}));

vi.mock("./horse-race-results-correlation-matrix", () => ({
  HorseRaceResultsCorrelationMatrix: ({ rows }: MatrixStubProps) => (
    <div data-testid="correlation-matrix-stub">{rows.length}</div>
  ),
}));

vi.mock("../../../lib/horse-weight-stream-client", () => ({
  useHorseWeightStream: vi.fn<() => HorseWeightSnapshot | null>(() => null),
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
  kishumeiRyakusho: "騎手",
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

const chartRunner = (overrides: Partial<HorseRaceChartRunner>): HorseRaceChartRunner => ({
  bataiju: "486",
  kettoTorokuBango: "2022100001",
  umaban: "01",
  wakuban: "1",
  zogenFugo: "+",
  zogenSa: "006",
  ...overrides,
});

test("renders the empty state when no results are provided", () => {
  render(<HorseRaceResultsChart results={[]} />);
  expect(screen.getByText("表示できる競走成績がありません").textContent).toStrictEqual(
    "表示できる競走成績がありません",
  );
  expect(screen.queryAllByTestId("line-chart-stub").length).toStrictEqual(0);
});

test("renders the empty state when every row has an invalid race date", () => {
  render(
    <HorseRaceResultsChart results={[chartResult({ kaisaiNen: "", kaisaiTsukihi: "0322" })]} />,
  );
  expect(screen.getByText("表示できる競走成績がありません").textContent).toStrictEqual(
    "表示できる競走成績がありません",
  );
  expect(screen.queryAllByTestId("line-chart-stub").length).toStrictEqual(0);
});

test("renders the four metric panels simultaneously in the fixed order", () => {
  render(<HorseRaceResultsChart results={[chartResult({})]} />);
  expect(
    screen.getAllByRole("heading", { level: 3 }).map((heading) => heading.textContent),
  ).toStrictEqual(["着順", "人気", "馬体重", "馬体重増減"]);
  expect(screen.getAllByTestId("line-chart-stub").length).toStrictEqual(4);
});

test("renders the view toggle with the overview mode pressed by default", () => {
  render(<HorseRaceResultsChart results={[chartResult({})]} />);
  expect(
    screen.getByRole("button", { name: "俯瞰（指標別）" }).getAttribute("aria-pressed"),
  ).toStrictEqual("true");
  expect(
    screen.getByRole("button", { name: "馬別（相関）" }).getAttribute("aria-pressed"),
  ).toStrictEqual("false");
  expect(screen.queryAllByTestId("correlation-matrix-stub").length).toStrictEqual(0);
});

test("renders the view toggle, bulk buttons and horse chips in the overview mode", () => {
  render(
    <HorseRaceResultsChart
      results={[
        chartResult({}),
        chartResult({
          bamei: "ベータ",
          currentUmaban: "02",
          kakuteiChakujun: "02",
          kettoTorokuBango: "2022100002",
          umaban: "02",
        }),
      ]}
    />,
  );
  expect(screen.getAllByRole("button").map((button) => button.textContent)).toStrictEqual([
    "俯瞰（指標別）",
    "馬別（相関）",
    "全馬表示",
    "全馬非表示",
    "1 アルファ",
    "2 ベータ",
  ]);
});

test("applies the shared sync id to all four overview panels", () => {
  render(<HorseRaceResultsChart results={[chartResult({})]} />);
  expect(
    screen.getAllByTestId("line-chart-stub").map((chart) => chart.getAttribute("data-sync-id")),
  ).toStrictEqual([
    "race-results-overview",
    "race-results-overview",
    "race-results-overview",
    "race-results-overview",
  ]);
});

test("reverses the Y axis for the rank panels and keeps it normal for the weight panels", () => {
  render(<HorseRaceResultsChart results={[chartResult({})]} />);
  expect(screen.getAllByTestId("y-axis-stub").map((axis) => axis.textContent)).toStrictEqual([
    "reversed",
    "reversed",
    "normal",
    "normal",
  ]);
});

test("renders one line per horse in every panel", () => {
  render(
    <HorseRaceResultsChart
      results={[
        chartResult({}),
        chartResult({
          bamei: "ベータ",
          currentUmaban: "02",
          kakuteiChakujun: "02",
          kettoTorokuBango: "2022100002",
          umaban: "02",
        }),
      ]}
    />,
  );
  expect(screen.getAllByTestId("line-stub").map((line) => line.textContent)).toStrictEqual([
    "1 アルファ",
    "2 ベータ",
    "1 アルファ",
    "2 ベータ",
    "1 アルファ",
    "2 ベータ",
    "1 アルファ",
    "2 ベータ",
  ]);
});

test("strokes a chart line with the entered-race frame color from the runner wakuban", () => {
  render(
    <HorseRaceResultsChart
      results={[chartResult({ wakuban: "3" })]}
      runners={[chartRunner({ wakuban: "3" })]}
      targetKeibajoCode="05"
      targetRaceDate="20260601"
    />,
  );
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-stroke")),
  ).toStrictEqual(["#d71920", "#d71920", "#d71920", "#d71920"]);
});

test("colors the chip swatch with the entered-race frame color", () => {
  const { container } = render(
    <HorseRaceResultsChart
      results={[chartResult({ wakuban: "3" })]}
      runners={[chartRunner({ wakuban: "3" })]}
      targetKeibajoCode="05"
      targetRaceDate="20260601"
    />,
  );
  const swatch = container.querySelector<HTMLElement>(".race-results-chart-swatch");
  expect(swatch?.style.backgroundColor).toStrictEqual("#d71920");
});

test("boosts the stroke width for the white frame and keeps the default width otherwise", () => {
  render(
    <HorseRaceResultsChart
      results={[
        chartResult({ wakuban: "1" }),
        chartResult({
          bamei: "ベータ",
          currentUmaban: "03",
          kakuteiChakujun: "02",
          kettoTorokuBango: "2022100002",
          umaban: "03",
          wakuban: "3",
        }),
      ]}
      runners={[
        chartRunner({ umaban: "01", wakuban: "1" }),
        chartRunner({ kettoTorokuBango: "2022100002", umaban: "03", wakuban: "3" }),
      ]}
      targetKeibajoCode="05"
      targetRaceDate="20260601"
    />,
  );
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-stroke-width")),
  ).toStrictEqual(["3.2", "2.4", "3.2", "2.4", "3.2", "2.4", "3.2", "2.4"]);
});

test("falls back to the palette color when no runner matches the horse", () => {
  render(<HorseRaceResultsChart results={[chartResult({})]} />);
  expect(
    screen.getAllByTestId("line-stub").map((line) => line.getAttribute("data-stroke")),
  ).toStrictEqual(["#d62728", "#d62728", "#d62728", "#d62728"]);
});

test("appends one upcoming point to the weight panel without adding it to the finish panel", () => {
  render(
    <HorseRaceResultsChart
      results={[chartResult({})]}
      runners={[chartRunner({})]}
      targetKeibajoCode="05"
      targetRaceDate="20260601"
    />,
  );
  const lines = screen.getAllByTestId("line-stub");
  expect(lines[0]?.getAttribute("data-total-points")).toStrictEqual("1");
  expect(lines[0]?.getAttribute("data-upcoming-points")).toStrictEqual("0");
  expect(lines[2]?.getAttribute("data-total-points")).toStrictEqual("2");
  expect(lines[2]?.getAttribute("data-upcoming-points")).toStrictEqual("1");
});

test("appends the realtime upcoming weight point to the weight panel but not the finish panel", () => {
  vi.mocked(useHorseWeightStream).mockReturnValue({
    fetchedAt: "2026-06-13T09:01:00Z",
    horses: [
      { changeAmount: 2, changeSign: null, horseName: "アルファ", horseNumber: "1", weight: 444 },
    ],
  });
  render(
    <HorseRaceResultsChart
      day="13"
      keibajoCode="09"
      month="06"
      raceNumber="01"
      results={[chartResult({})]}
      runners={[chartRunner({})]}
      source="jra"
      targetKeibajoCode="09"
      targetRaceDate="20260613"
      year="2026"
    />,
  );
  const lines = screen.getAllByTestId("line-stub");
  expect(lines[0]?.getAttribute("data-total-points")).toStrictEqual("1");
  expect(lines[0]?.getAttribute("data-upcoming-points")).toStrictEqual("0");
  expect(lines[2]?.getAttribute("data-total-points")).toStrictEqual("2");
  expect(lines[2]?.getAttribute("data-upcoming-points")).toStrictEqual("1");
});

test("renders no upcoming weight point when the realtime stream has no snapshot", () => {
  vi.mocked(useHorseWeightStream).mockReturnValue(null);
  render(
    <HorseRaceResultsChart
      day="13"
      keibajoCode="09"
      month="06"
      raceNumber="01"
      results={[chartResult({})]}
      source="jra"
      targetKeibajoCode="09"
      targetRaceDate="20260613"
      year="2026"
    />,
  );
  const lines = screen.getAllByTestId("line-stub");
  expect(lines[2]?.getAttribute("data-total-points")).toStrictEqual("1");
  expect(lines[2]?.getAttribute("data-upcoming-points")).toStrictEqual("0");
});

test("renders distance and jockey in the finish and popularity tooltips only", () => {
  render(<HorseRaceResultsChart results={[chartResult({})]} />);
  const tooltips = screen.getAllByTestId("tooltip-stub");
  expect(tooltips.map((tooltip) => tooltip.getAttribute("data-has-content"))).toStrictEqual([
    "true",
    "true",
    "false",
    "false",
  ]);
  expect(screen.getAllByText("距離 2000m").length).toStrictEqual(2);
  expect(screen.getAllByText("騎手 ルメール").length).toStrictEqual(2);
});

test("hides one horse in every panel via its chip and shows it again on the second click", () => {
  render(
    <HorseRaceResultsChart
      results={[
        chartResult({}),
        chartResult({
          bamei: "ベータ",
          currentUmaban: "02",
          kakuteiChakujun: "02",
          kettoTorokuBango: "2022100002",
          umaban: "02",
        }),
      ]}
    />,
  );
  const alphaChip = screen.getByRole("button", { name: "1 アルファ" });
  expect(alphaChip.getAttribute("aria-pressed")).toStrictEqual("true");
  fireEvent.click(alphaChip);
  expect(
    screen.getByRole("button", { name: "1 アルファ" }).getAttribute("aria-pressed"),
  ).toStrictEqual("false");
  expect(screen.getAllByTestId("line-stub").map((line) => line.textContent)).toStrictEqual([
    "2 ベータ",
    "2 ベータ",
    "2 ベータ",
    "2 ベータ",
  ]);
  fireEvent.click(screen.getByRole("button", { name: "1 アルファ" }));
  expect(
    screen.getByRole("button", { name: "1 アルファ" }).getAttribute("aria-pressed"),
  ).toStrictEqual("true");
  expect(screen.getAllByTestId("line-stub").map((line) => line.textContent)).toStrictEqual([
    "1 アルファ",
    "2 ベータ",
    "1 アルファ",
    "2 ベータ",
    "1 アルファ",
    "2 ベータ",
    "1 アルファ",
    "2 ベータ",
  ]);
});

test("hides every horse in every panel via 全馬非表示 and restores them via 全馬表示", () => {
  render(
    <HorseRaceResultsChart
      results={[
        chartResult({}),
        chartResult({
          bamei: "ベータ",
          currentUmaban: "02",
          kakuteiChakujun: "02",
          kettoTorokuBango: "2022100002",
          umaban: "02",
        }),
      ]}
    />,
  );
  fireEvent.click(screen.getByRole("button", { name: "全馬非表示" }));
  expect(screen.queryAllByTestId("line-stub").length).toStrictEqual(0);
  expect(
    screen.getByRole("button", { name: "1 アルファ" }).getAttribute("aria-pressed"),
  ).toStrictEqual("false");
  expect(
    screen.getByRole("button", { name: "2 ベータ" }).getAttribute("aria-pressed"),
  ).toStrictEqual("false");
  fireEvent.click(screen.getByRole("button", { name: "全馬表示" }));
  expect(screen.getAllByTestId("line-stub").map((line) => line.textContent)).toStrictEqual([
    "1 アルファ",
    "2 ベータ",
    "1 アルファ",
    "2 ベータ",
    "1 アルファ",
    "2 ベータ",
    "1 アルファ",
    "2 ベータ",
  ]);
  expect(
    screen.getByRole("button", { name: "1 アルファ" }).getAttribute("aria-pressed"),
  ).toStrictEqual("true");
  expect(
    screen.getByRole("button", { name: "2 ベータ" }).getAttribute("aria-pressed"),
  ).toStrictEqual("true");
});

test("switches to the correlation view and back to the overview", () => {
  render(
    <HorseRaceResultsChart
      results={[
        chartResult({}),
        chartResult({ kaisaiTsukihi: "0510", raceBango: "02" }),
        chartResult({
          bamei: "ベータ",
          currentUmaban: "02",
          kaisaiTsukihi: "0405",
          kakuteiChakujun: "02",
          kettoTorokuBango: "2022100002",
          umaban: "02",
        }),
      ]}
    />,
  );
  fireEvent.click(screen.getByRole("button", { name: "馬別（相関）" }));
  expect(
    screen.getByRole("button", { name: "馬別（相関）" }).getAttribute("aria-pressed"),
  ).toStrictEqual("true");
  expect(
    screen.getByRole("button", { name: "俯瞰（指標別）" }).getAttribute("aria-pressed"),
  ).toStrictEqual("false");
  expect(screen.queryAllByTestId("line-chart-stub").length).toStrictEqual(0);
  expect(screen.getByTestId("correlation-matrix-stub").textContent).toStrictEqual("2");
  fireEvent.click(screen.getByRole("button", { name: "俯瞰（指標別）" }));
  expect(screen.getAllByTestId("line-chart-stub").length).toStrictEqual(4);
  expect(screen.queryAllByTestId("correlation-matrix-stub").length).toStrictEqual(0);
});

test("appends the upcoming weight column to the correlation matrix rows", () => {
  render(
    <HorseRaceResultsChart
      results={[chartResult({})]}
      runners={[chartRunner({})]}
      targetKeibajoCode="05"
      targetRaceDate="20260601"
    />,
  );
  fireEvent.click(screen.getByRole("button", { name: "馬別（相関）" }));
  expect(screen.getByTestId("correlation-matrix-stub").textContent).toStrictEqual("2");
});

test("appends the realtime upcoming weight column to the correlation matrix rows", () => {
  vi.mocked(useHorseWeightStream).mockReturnValue({
    fetchedAt: "2026-06-13T09:01:00Z",
    horses: [
      { changeAmount: 2, changeSign: null, horseName: "アルファ", horseNumber: "1", weight: 444 },
    ],
  });
  render(
    <HorseRaceResultsChart
      day="13"
      keibajoCode="09"
      month="06"
      raceNumber="01"
      results={[chartResult({})]}
      runners={[chartRunner({})]}
      source="jra"
      targetKeibajoCode="09"
      targetRaceDate="20260613"
      year="2026"
    />,
  );
  fireEvent.click(screen.getByRole("button", { name: "馬別（相関）" }));
  expect(screen.getByTestId("correlation-matrix-stub").textContent).toStrictEqual("2");
});

test("hides the bulk buttons and single-selects the first horse in the correlation view", () => {
  render(
    <HorseRaceResultsChart
      results={[
        chartResult({}),
        chartResult({
          bamei: "ベータ",
          currentUmaban: "02",
          kaisaiTsukihi: "0405",
          kakuteiChakujun: "02",
          kettoTorokuBango: "2022100002",
          umaban: "02",
        }),
      ]}
    />,
  );
  fireEvent.click(screen.getByRole("button", { name: "馬別（相関）" }));
  expect(screen.getAllByRole("button").map((button) => button.textContent)).toStrictEqual([
    "俯瞰（指標別）",
    "馬別（相関）",
    "1 アルファ",
    "2 ベータ",
  ]);
  expect(
    screen.getByRole("button", { name: "1 アルファ" }).getAttribute("aria-pressed"),
  ).toStrictEqual("true");
  expect(
    screen.getByRole("button", { name: "2 ベータ" }).getAttribute("aria-pressed"),
  ).toStrictEqual("false");
});

test("switches the correlated horse when another chip is clicked", () => {
  render(
    <HorseRaceResultsChart
      results={[
        chartResult({}),
        chartResult({ kaisaiTsukihi: "0510", raceBango: "02" }),
        chartResult({
          bamei: "ベータ",
          currentUmaban: "02",
          kaisaiTsukihi: "0405",
          kakuteiChakujun: "02",
          kettoTorokuBango: "2022100002",
          umaban: "02",
        }),
      ]}
    />,
  );
  fireEvent.click(screen.getByRole("button", { name: "馬別（相関）" }));
  expect(screen.getByTestId("correlation-matrix-stub").textContent).toStrictEqual("2");
  fireEvent.click(screen.getByRole("button", { name: "2 ベータ" }));
  expect(screen.getByTestId("correlation-matrix-stub").textContent).toStrictEqual("1");
  expect(
    screen.getByRole("button", { name: "2 ベータ" }).getAttribute("aria-pressed"),
  ).toStrictEqual("true");
  expect(
    screen.getByRole("button", { name: "1 アルファ" }).getAttribute("aria-pressed"),
  ).toStrictEqual("false");
  fireEvent.click(screen.getByRole("button", { name: "2 ベータ" }));
  expect(screen.getByTestId("correlation-matrix-stub").textContent).toStrictEqual("1");
  expect(
    screen.getByRole("button", { name: "2 ベータ" }).getAttribute("aria-pressed"),
  ).toStrictEqual("true");
});

test("passes zero correlation rows when no row has a ketto number", () => {
  render(<HorseRaceResultsChart results={[chartResult({ kettoTorokuBango: " " })]} />);
  fireEvent.click(screen.getByRole("button", { name: "馬別（相関）" }));
  expect(screen.getAllByRole("button").map((button) => button.textContent)).toStrictEqual([
    "俯瞰（指標別）",
    "馬別（相関）",
  ]);
  expect(screen.getByTestId("correlation-matrix-stub").textContent).toStrictEqual("0");
});
