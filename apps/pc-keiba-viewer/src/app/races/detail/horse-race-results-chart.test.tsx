// Run with: bunx vitest run src/app/races/detail/horse-race-results-chart.test.tsx

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import React from "react";
import type { ReactNode } from "react";
import { afterEach, expect, test, vi } from "vitest";

import type { HorseRaceResult } from "../../../lib/race-types";
import { HorseRaceResultsChart } from "./horse-race-results-chart";

interface ChartChildrenStubProps {
  children?: ReactNode;
}

interface LineChartStubProps {
  children?: ReactNode;
  syncId?: string;
}

interface LineStubProps {
  name?: string;
}

interface YAxisStubProps {
  reversed?: boolean;
}

interface MatrixStubProps {
  rows: unknown[];
}

vi.mock("recharts", () => ({
  CartesianGrid: () => <div data-testid="cartesian-grid-stub" />,
  Line: ({ name }: LineStubProps) => <div data-testid="line-stub">{name}</div>,
  LineChart: ({ children, syncId }: LineChartStubProps) => (
    <div data-sync-id={syncId} data-testid="line-chart-stub">
      {children}
    </div>
  ),
  ResponsiveContainer: ({ children }: ChartChildrenStubProps) => (
    <div data-testid="responsive-container-stub">{children}</div>
  ),
  Tooltip: () => <div data-testid="tooltip-stub" />,
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
