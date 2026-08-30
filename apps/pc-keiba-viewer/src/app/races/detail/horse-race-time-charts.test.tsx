// This file runs with bun.

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";

import type {
  HorseRaceResult,
  RaceTimeStats,
  RaceTimeTargetRace,
  Runner,
} from "../../../lib/race-types";
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

const targetRace = (overrides: Partial<RaceTimeTargetRace>): RaceTimeTargetRace => ({
  date: "20260301",
  horseName: "1着馬",
  horseNumber: "01",
  jockeyName: "騎手",
  keibajoCode: "43",
  kohan3f: "357",
  kyori: "1700",
  ownerName: "馬主",
  popularity: "01",
  raceName: "一般",
  raceNumber: "11",
  raceTime: "1050",
  trainerName: "調教師",
  ...overrides,
});

const runner = (overrides: Partial<Runner>): Runner => ({
  bamei: "出走馬",
  banushimei: "馬主",
  barei: "04",
  bataiju: "480",
  chokyoshimeiRyakusho: "調教師",
  damSireName: null,
  futanJuryo: "262",
  kakuteiChakujun: null,
  kettoTorokuBango: "2022100001",
  kishumeiRyakusho: "騎手",
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  kohan3f: null,
  seibetsuCode: "1",
  sireName: null,
  sireSireName: null,
  sohaTime: null,
  tanshoNinkijun: null,
  tanshoOdds: null,
  timeSa: null,
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
  targetRaces: [
    targetRace({ kyori: "1000", raceTime: "575" }),
    targetRace({ date: "20260308", kyori: "1800", raceTime: "1100" }),
  ],
});

it("renders the non-Ban-ei scatter with same-condition fastest average and median lines", () => {
  render(
    <HorseRaceTimeChart
      currentDistance="1800"
      results={[
        result({}),
        result({ bamei: "2着馬", currentUmaban: "02", kakuteiChakujun: "02", umaban: "02" }),
      ]}
      runners={[]}
      stats={stats()}
    />,
  );
  expect(screen.getByRole("figure", { name: "競走成績タイム散布図" })).toBeDefined();
  expect(screen.getByText("換算タイム×上がり3F")).toBeDefined();
  expect(screen.getByText("最速レースタイム 1:45.0")).toBeDefined();
  expect(screen.getByText("平均レースタイム 1:52.0")).toBeDefined();
  expect(screen.getByText("中央値レースタイム 1:48.0")).toBeDefined();
  expect(screen.getByText("最速上がり3F 34.0")).toBeDefined();
  expect(screen.getByText("平均上がり3F 36.0")).toBeDefined();
  expect(screen.getByText("中央値上がり3F 35.5")).toBeDefined();
  expect(document.querySelectorAll("circle.training-chart-point").length).toBe(2);
  expect(screen.queryByText("換算レースタイム（今走距離、上が速い）")).toBeNull();
});

it("shows an empty message when clocks cannot be plotted", () => {
  render(
    <HorseRaceTimeChart
      currentDistance="1800"
      keibajoCode="05"
      results={[result({ kohan3f: "000", sohaTime: "0000" })]}
      runners={[]}
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
      runners={[runner({ futanJuryo: "262" })]}
      stats={null}
    />,
  );
  expect(screen.getByRole("figure", { name: "競走成績タイム散布図" })).toBeDefined();
  expect(screen.getByText("斤量（右が重い）")).toBeDefined();
  expect(screen.queryByText("着順（右が上位）")).toBeNull();
  expect(
    screen.getByText(
      "ばんえいには上がり3Fがありません。1つの図で斤量・換算タイム・着順を見ます。上ほど速く、右ほど斤量が重い。点の中の数字と色・大きさが着順、右の数字は馬番。◇は今走の予定斤量、横線は過去斤量との差。同じ馬の複数レースは薄い線でつなぎます。",
    ),
  ).toBeDefined();
  expect(screen.getByText("過去斤量")).toBeDefined();
  expect(screen.getByText("予定斤量")).toBeDefined();
  expect(screen.getByText("予定斤量 610kg")).toBeDefined();
  expect(screen.queryByText("上がり3F（右が速い）")).toBeNull();
  expect(document.querySelectorAll("[data-horse-link='1']").length).toBe(1);
  expect(document.querySelectorAll("[data-weight-link='1']").length).toBe(1);
  expect(document.querySelectorAll("[data-scheduled-weight='1']").length).toBe(1);
  expect(document.querySelectorAll("[data-finish-label='1']").length).toBe(2);
  const firstMark = document.querySelector('[data-finish="1"]');
  expect(firstMark?.getAttribute("r")).toBe("10");
  if (firstMark === null) {
    throw new Error("expected a Ban-ei finish mark");
  }
  fireEvent.pointerEnter(firstMark, { clientX: 40, clientY: 48 });
  expect(firstMark.getAttribute("r")).toBe("11.4");
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
      runners={[]}
      stats={null}
    />,
  );
  expect(screen.getByText("レースタイムと着順と斤量が揃った競走成績がありません。")).toBeDefined();
  expect(screen.queryByRole("figure", { name: "競走成績タイム散布図" })).toBeNull();
});

it("follows the pointer with a scaled-clock gallery tooltip", () => {
  render(
    <HorseRaceTimeChart currentDistance="1800" results={[result({})]} runners={[]} stats={null} />,
  );
  const hoverPoint = document.querySelector('[data-horse="チャートホース"]');
  if (hoverPoint === null) {
    throw new Error("expected a scatter point");
  }
  fireEvent.pointerEnter(hoverPoint, { clientX: 40, clientY: 48 });
  expect(screen.getByRole("tooltip")).toBeDefined();
  expect(screen.getByText("1 チャートホース")).toBeDefined();
  expect(screen.getByText("換算タイム 1:50.0")).toBeDefined();
  expect(screen.getByText("上がり3F 35.1")).toBeDefined();
  fireEvent.pointerLeave(hoverPoint);
  expect(screen.queryByRole("tooltip")).toBeNull();
});

it("still plots both horses when the other horse ran a different distance", () => {
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
      runners={[]}
      stats={null}
    />,
  );
  expect(document.querySelector('[data-horse="同距離"]') === null).toBe(false);
  expect(document.querySelector('[data-horse="短い距離"]') === null).toBe(false);
});

it("dims the other horse and enlarges the hovered gallery point", () => {
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
      runners={[]}
      stats={null}
    />,
  );
  const scatter = screen.getByRole("figure", { name: "競走成績タイム散布図" });
  const hoverPoint = scatter.querySelector('[data-horse="ホバー馬"]');
  const otherPoint = scatter.querySelector('[data-horse="別の馬"]');
  if (hoverPoint === null || otherPoint === null) {
    throw new Error("expected both scatter points");
  }
  fireEvent.pointerEnter(hoverPoint, { clientX: 40, clientY: 48 });
  expect(hoverPoint.getAttribute("r")).toBe("7");
  expect(otherPoint.getAttribute("fill-opacity")).toBe("0.08");
  expect(scatter.querySelector('[data-umaban-label="1"]')?.getAttribute("opacity")).toBe("1");
  expect(scatter.querySelector('[data-umaban-label="2"]')?.getAttribute("opacity")).toBe("0.22");
});

it("dims the other Ban-ei scheduled-weight mark while hovering a horse", () => {
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
          bamei: "別の馬",
          currentUmaban: "02",
          futanJuryo: "262",
          keibajoCode: "83",
          kohan3f: "000",
          kyori: "200",
          sohaTime: "3300",
          umaban: "02",
        }),
      ]}
      runners={[runner({ futanJuryo: "26C" }), runner({ futanJuryo: "276", umaban: "02" })]}
      stats={null}
    />,
  );
  const hoverPoint = document.querySelector('[data-horse="チャートホース"]');
  const hoverLink = document.querySelector('[data-weight-link="1"]');
  const otherLink = document.querySelector('[data-weight-link="2"]');
  const otherMark = document.querySelector('[data-scheduled-weight="2"]');
  if (hoverPoint === null || hoverLink === null || otherLink === null || otherMark === null) {
    throw new Error("expected Ban-ei weight marks");
  }
  expect(hoverLink.getAttribute("stroke-opacity")).toBe("0.38");
  fireEvent.pointerEnter(hoverPoint, { clientX: 40, clientY: 48 });
  expect(hoverLink.getAttribute("stroke-opacity")).toBe("0.62");
  expect(otherLink.getAttribute("stroke-opacity")).toBe("0.08");
  expect(otherMark.getAttribute("stroke-opacity")).toBe("0.1");
  fireEvent.pointerEnter(otherMark, { clientX: 48, clientY: 52 });
  expect(screen.getByText("予定斤量 630kg")).toBeDefined();
  expect(screen.getByText("斤量差 -20kg")).toBeDefined();
});
