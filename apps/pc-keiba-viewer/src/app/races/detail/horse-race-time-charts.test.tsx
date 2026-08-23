// This file runs with bun.

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";

import type { HorseRaceResult, RaceTimeStats } from "../../../lib/race-types";
import { HorseRaceTimeChart } from "./horse-race-time-charts";

afterEach(cleanup);

const result = (overrides: Partial<HorseRaceResult>): HorseRaceResult => ({
  babajotaiCodeDirt: "1",
  babajotaiCodeShiba: "0",
  banushimei: "馬主",
  barei: "04",
  bataiju: "480",
  bamei: "チャートホース",
  chokyoshimeiRyakusho: "調教師",
  currentBarei: "04",
  currentJockey: "騎手",
  currentSeibetsuCode: "1",
  currentUmaban: "01",
  corner1: "03",
  corner2: "04",
  corner3: "05",
  corner4: "06",
  futanJuryo: "550",
  gradeCode: "00",
  hassoJikoku: "1200",
  jockeyName: "騎手",
  juryoShubetsuCode: "1",
  kakuteiChakujun: "01",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0322",
  keibajoCode: "05",
  kettoTorokuBango: "2022100001",
  kishumeiRyakusho: "騎手",
  kohan3f: "351",
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
  sohaTime: "1100",
  tanshoNinkijun: "03",
  tanshoOdds: "45",
  tenkoCode: "1",
  timeSa: "002",
  trackCode: "24",
  umaban: "01",
  wakuban: "1",
  zogenFugo: null,
  zogenSa: null,
  ...overrides,
});

const stats = (): RaceTimeStats => ({
  averageKohan3f: 360,
  averageRaceTime: 1120,
  correlationRows: [],
  fastestDetail: null,
  fastestKohan3f: 340,
  fastestRaceTime: 1050,
  medianKohan3f: 355,
  medianRaceTime: 1080,
  raceCount: 10,
  targetRaces: [],
});

it("renders the race-time scatter with finish colors and reference lines", () => {
  render(
    <HorseRaceTimeChart
      currentDistance="1800"
      results={[
        result({}),
        result({ bamei: "2着馬", currentUmaban: "02", kakuteiChakujun: "02", umaban: "02" }),
      ]}
      stats={stats()}
    />,
  );
  expect(screen.getByRole("figure", { name: "競走成績タイム散布図" })).toBeDefined();
  expect(screen.getByText("上がり3F（右が速い）")).toBeDefined();
  expect(screen.getByText("換算レースタイム（今走距離、上が速い）")).toBeDefined();
  expect(screen.getByText("1着")).toBeDefined();
  expect(screen.getByText("最速")).toBeDefined();
  expect(document.querySelectorAll("circle.training-chart-point").length).toBe(2);
  expect(document.querySelector('[data-finish="2"]')?.getAttribute("r")).toBe("4.4");
  expect(document.querySelectorAll("[data-reference]").length).toBe(6);
  expect(document.querySelector("[data-reference]")?.getAttribute("stroke-opacity")).toBe("0.1");
  expect(document.querySelector("[data-reference]")?.getAttribute("stroke-width")).toBe("0.45");
  expect(
    document.querySelector('[data-horse="チャートホース"]')?.getAttribute("fill-opacity"),
  ).toBe("0.4");
  expect(
    document.querySelector('[data-horse="チャートホース"]')?.getAttribute("stroke-opacity"),
  ).toBe("0.42");
  expect(document.querySelector('[data-umaban-label="1"]')?.getAttribute("opacity")).toBe("0.92");
  expect(document.querySelector('[data-reference="fastestRaceTime"]') === null).toBe(false);
  expect(document.querySelector('[data-reference="medianKohan3f"]') === null).toBe(false);
});

it("shows an empty message when clocks cannot be plotted", () => {
  render(
    <HorseRaceTimeChart
      currentDistance="1800"
      keibajoCode="05"
      results={[result({ kohan3f: "000", sohaTime: "0000" })]}
      stats={null}
    />,
  );
  expect(screen.getByText("レースタイムと上がり3Fが揃った競走成績がありません。")).toBeDefined();
  expect(screen.queryByRole("figure", { name: "競走成績タイム散布図" })).toBeNull();
});

it("plots Ban-ei clocks on a finish-rank axis without last 3F", () => {
  render(
    <HorseRaceTimeChart
      currentDistance="200"
      keibajoCode="83"
      results={[
        result({
          futanJuryo: "262",
          keibajoCode: "83",
          kohan3f: "000",
          kyori: "200",
          sohaTime: "3188",
        }),
        result({
          futanJuryo: "26C",
          kaisaiTsukihi: "0401",
          keibajoCode: "83",
          kohan3f: "000",
          kyori: "200",
          sohaTime: "3300",
        }),
      ]}
      stats={null}
    />,
  );
  expect(screen.getByRole("figure", { name: "競走成績タイム散布図" })).toBeDefined();
  expect(screen.getByText("斤量（右が重い）")).toBeDefined();
  expect(screen.queryByText("着順（右が上位）")).toBeNull();
  expect(
    screen.getByText(
      "ばんえいには上がり3Fがありません。1つの図で斤量・換算タイム・着順を見ます。上ほど速く、右ほど斤量が重い。点の色と大きさは着順、数字は馬番。同じ馬の複数レースは薄い線でつなぎます。",
    ),
  ).toBeDefined();
  expect(screen.queryByText("上がり3F（右が速い）")).toBeNull();
  expect(document.querySelectorAll("[data-horse-link='1']").length).toBe(1);
  const firstMark = document.querySelector('[data-finish="1"]');
  expect(firstMark?.getAttribute("r")).toBe("7");
  if (firstMark === null) {
    throw new Error("expected a Ban-ei finish mark");
  }
  fireEvent.pointerEnter(firstMark, { clientX: 40, clientY: 48 });
  expect(firstMark.getAttribute("r")).toBe("8.4");
});

it("shows a Ban-ei empty message when finish ranks are missing", () => {
  render(
    <HorseRaceTimeChart
      currentDistance="200"
      keibajoCode="83"
      results={[
        result({
          kakuteiChakujun: "00",
          keibajoCode: "83",
          kohan3f: "000",
          kyori: "200",
          sohaTime: "3188",
        }),
      ]}
      stats={null}
    />,
  );
  expect(screen.getByText("レースタイムと着順と斤量が揃った競走成績がありません。")).toBeDefined();
  expect(screen.queryByRole("figure", { name: "競走成績タイム散布図" })).toBeNull();
});

it("follows the pointer with a tooltip of horse, clocks, and finish", () => {
  render(<HorseRaceTimeChart currentDistance="1800" results={[result({})]} stats={null} />);
  const hoverPoint = document.querySelector('[data-horse="チャートホース"]');
  if (hoverPoint === null) {
    throw new Error("expected a scatter point");
  }
  fireEvent.pointerEnter(hoverPoint, { clientX: 40, clientY: 48 });
  expect(screen.getByRole("tooltip")).toBeDefined();
  expect(screen.getByText("1 チャートホース")).toBeDefined();
  expect(screen.getByText("過去騎手 騎手")).toBeDefined();
  expect(screen.getByText("予定騎手 騎手")).toBeDefined();
  expect(screen.getByText("着順 1")).toBeDefined();
  expect(screen.getByText("レースタイム 1:50.0")).toBeDefined();
  expect(screen.getByText("距離 1800m")).toBeDefined();
  expect(screen.getByText("上がり3F 35.1")).toBeDefined();
  fireEvent.pointerLeave(hoverPoint);
  expect(screen.queryByRole("tooltip")).toBeNull();
});

it("fades a point whose race distance is farther from the current race", () => {
  render(
    <HorseRaceTimeChart
      currentDistance="1800"
      results={[
        result({ bamei: "同距離", kyori: "1800" }),
        result({
          bamei: "短い距離",
          currentUmaban: "02",
          kyori: "1600",
          umaban: "02",
        }),
      ]}
      stats={null}
    />,
  );
  expect(document.querySelector('[data-horse="同距離"]')?.getAttribute("fill-opacity")).toBe("0.4");
  expect(document.querySelector('[data-horse="短い距離"]')?.getAttribute("fill-opacity")).toBe(
    "0.3",
  );
  expect(document.querySelector('[data-umaban-label="1"]')?.getAttribute("opacity")).toBe("0.92");
  expect(document.querySelector('[data-umaban-label="2"]')?.getAttribute("opacity")).toBe("0.92");
});

it("dims the other horse and enlarges the hovered point", () => {
  render(
    <HorseRaceTimeChart
      currentDistance="1800"
      results={[
        result({ bamei: "ホバー馬", currentUmaban: "01" }),
        result({
          bamei: "別の馬",
          currentUmaban: "02",
          kakuteiChakujun: "08",
          umaban: "02",
        }),
      ]}
      stats={null}
    />,
  );
  const hoverPoint = document.querySelector('[data-horse="ホバー馬"]');
  const otherPoint = document.querySelector('[data-horse="別の馬"]');
  if (hoverPoint === null || otherPoint === null) {
    throw new Error("expected both scatter points");
  }
  fireEvent.pointerEnter(hoverPoint, { clientX: 40, clientY: 48 });
  expect(hoverPoint.getAttribute("r")).toBe("6.2");
  expect(hoverPoint.getAttribute("stroke-opacity")).toBe("0.7");
  expect(otherPoint.getAttribute("fill-opacity")).toBe("0.1");
  expect(otherPoint.getAttribute("stroke-opacity")).toBe("0.1");
  expect(otherPoint.getAttribute("r")).toBe("3.2");
  expect(document.querySelector('[data-umaban-label="1"]')?.getAttribute("opacity")).toBe("1");
  expect(document.querySelector('[data-umaban-label="2"]')?.getAttribute("opacity")).toBe("0.26");
});
