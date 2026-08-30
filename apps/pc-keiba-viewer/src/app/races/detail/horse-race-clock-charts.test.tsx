// This file runs with bun.

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";

import type { HorseRaceResult, RaceTimeStats } from "../../../lib/race-types";
import { HorseRaceClockGallery } from "./horse-race-clock-charts";

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
  kyori: "1700",
  kyosoJokenCode: "005",
  kyosoJokenMeisho: "3歳",
  kyosoKigoCode: "000",
  kyosoShubetsuCode: "12",
  kyosomeiFukudai: null,
  kyosomeiHondai: "テストレース",
  kyosomeiKakkonai: null,
  raceBango: "01",
  seibetsuCode: "1",
  sohaTime: "1050",
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
  averageKohan3f: 392,
  averageRaceTime: 1490,
  correlationRows: [],
  fastestDetail: null,
  fastestKohan3f: 356,
  fastestRaceTime: 1436,
  medianKohan3f: 391.5,
  medianRaceTime: 1489,
  raceCount: 400,
  targetRaces: [],
});

it("renders the filtered-race scatter with same-condition fastest average and median lines", () => {
  render(
    <HorseRaceClockGallery
      currentDistance="1700"
      results={[
        result({}),
        result({
          bamei: "2着馬",
          currentUmaban: "02",
          kakuteiChakujun: "02",
          umaban: "02",
        }),
      ]}
      runners={[]}
      stats={stats()}
    />,
  );
  expect(screen.getByRole("figure", { name: "競走成績タイム散布図" })).toBeDefined();
  expect(screen.getByRole("figure", { name: "競走成績タイム散布図" }).className).toBe(
    "training-chart race-clock-gallery",
  );
  expect(screen.getByText("換算タイム×上がり3F")).toBeDefined();
  expect(screen.queryByText("A. 最新走の換算タイム×上がり3F")).toBeNull();
  expect(
    screen.queryByText(
      "1頭1点（最新走）。右上が速い。線は同条件1着の最速・平均・中央値（レースタイムと上がり3F）。",
    ),
  ).toBeNull();
  expect(screen.getByText("最速レースタイム 2:23.6").closest("li") === null).toBe(false);
  expect(screen.getByText("平均レースタイム 2:29.0").closest("li") === null).toBe(false);
  expect(screen.getByText("中央値レースタイム 2:28.9").closest("li") === null).toBe(false);
  expect(screen.getByText("最速上がり3F 35.6").closest("li") === null).toBe(false);
  expect(screen.getByText("平均上がり3F 39.2").closest("li") === null).toBe(false);
  expect(screen.getByText("中央値上がり3F 39.1").closest("li") === null).toBe(false);
  expect(screen.queryByText("最速レースタイム 57.5")).toBeNull();
  expect(document.querySelectorAll(".training-chart-legend").length).toBe(2);
  expect(document.querySelector('[data-reference="fastestTime"]')?.tagName).toBe("line");
  expect(document.querySelector('[data-reference="fastestTime"]')?.getAttribute("stroke")).toBe(
    "#be123c",
  );
  expect(
    document.querySelector('[data-reference="fastestTime"]')?.getAttribute("stroke-dasharray"),
  ).toBe("6 4");
  expect(document.querySelector('[data-reference="averageTime"]')?.getAttribute("stroke")).toBe(
    "#166534",
  );
  expect(document.querySelector('[data-reference="medianTime"]')?.getAttribute("stroke")).toBe(
    "#4338ca",
  );
  expect(document.querySelector('[data-reference="fastestKohan"]')?.getAttribute("stroke")).toBe(
    "#be123c",
  );
  expect(
    document.querySelector('[data-reference="fastestKohan"]')?.getAttribute("stroke-dasharray"),
  ).toBe("2 3");
  expect(document.querySelector('[data-reference="averageKohan"]') === null).toBe(false);
  expect(document.querySelector('[data-reference="medianKohan"]') === null).toBe(false);
  expect(
    document.querySelector('[data-reference="fastestTime"]')?.getAttribute("stroke-opacity"),
  ).toBe("0.22");
  expect(document.querySelector('[data-reference-hit="fastestTime"]') === null).toBe(false);
  expect(document.querySelector(".training-chart-svg text.training-chart-umaban") === null).toBe(
    false,
  );
  expect(document.querySelectorAll("circle.training-chart-point").length).toBe(2);
  expect(document.querySelector('[data-horse="チャートホース"]')?.getAttribute("fill")).toBe(
    "#ffffff",
  );
  expect(document.querySelector('[data-horse="チャートホース"]')?.getAttribute("stroke")).toBe(
    "#111111",
  );
});

it("keeps the race-time legend when same-condition last-3F stats are missing", () => {
  render(
    <HorseRaceClockGallery
      currentDistance="1700"
      results={[result({})]}
      runners={[]}
      stats={{
        averageKohan3f: null,
        averageRaceTime: 1490,
        correlationRows: [],
        fastestDetail: null,
        fastestKohan3f: null,
        fastestRaceTime: 1436,
        medianKohan3f: null,
        medianRaceTime: 1489,
        raceCount: 400,
        targetRaces: [],
      }}
    />,
  );
  expect(screen.getByText("最速レースタイム 2:23.6").closest("li") === null).toBe(false);
  expect(screen.queryByText("最速上がり3F 35.6")).toBeNull();
  expect(document.querySelectorAll(".training-chart-legend").length).toBe(1);
  expect(document.querySelector('[data-reference="fastestKohan"]') === null).toBe(true);
});

it("keeps the last-3F legend when same-condition race-time stats are missing", () => {
  render(
    <HorseRaceClockGallery
      currentDistance="1700"
      results={[result({})]}
      runners={[]}
      stats={{
        averageKohan3f: 392,
        averageRaceTime: null,
        correlationRows: [],
        fastestDetail: null,
        fastestKohan3f: 356,
        fastestRaceTime: null,
        medianKohan3f: 391.5,
        medianRaceTime: null,
        raceCount: 400,
        targetRaces: [],
      }}
    />,
  );
  expect(screen.getByText("最速上がり3F 35.6").closest("li") === null).toBe(false);
  expect(screen.queryByText("最速レースタイム 2:23.6")).toBeNull();
  expect(document.querySelectorAll(".training-chart-legend").length).toBe(1);
  expect(document.querySelector('[data-reference="fastestTime"]') === null).toBe(true);
});

it("fills scatter points with the current-race frame color", () => {
  render(
    <HorseRaceClockGallery
      currentDistance="1700"
      results={[result({ kettoTorokuBango: "2022100001", wakuban: "1" })]}
      runners={[{ kettoTorokuBango: "2022100001", wakuban: "4" }]}
      stats={null}
    />,
  );
  expect(document.querySelector('[data-horse="チャートホース"]')?.getAttribute("fill")).toBe(
    "#005bac",
  );
  expect(document.querySelectorAll(".training-chart-legend").length).toBe(0);
});

it("shows an empty message when clocks cannot be plotted", () => {
  render(
    <HorseRaceClockGallery
      currentDistance="1700"
      results={[result({ kohan3f: "000", sohaTime: "0000" })]}
      runners={[]}
      stats={null}
    />,
  );
  expect(screen.getByText("レースタイムと上がり3Fが揃った競走成績がありません。")).toBeDefined();
});

it("follows the pointer with a scaled-clock tooltip", () => {
  render(
    <HorseRaceClockGallery
      currentDistance="1700"
      results={[result({})]}
      runners={[]}
      stats={null}
    />,
  );
  const hoverPoint = document.querySelector('[data-horse="チャートホース"]');
  if (hoverPoint === null) {
    throw new Error("expected a scatter point");
  }
  fireEvent.pointerEnter(hoverPoint, { clientX: 40, clientY: 48 });
  expect(screen.getByRole("tooltip")).toBeDefined();
  expect(screen.getByText("1 チャートホース")).toBeDefined();
  expect(screen.getByText("換算タイム 1:45.0")).toBeDefined();
  expect(screen.getByText("上がり3F 35.1")).toBeDefined();
  fireEvent.pointerLeave(hoverPoint);
  expect(screen.queryByRole("tooltip")).toBeNull();
});

it("plots every filtered race for the same horse", () => {
  render(
    <HorseRaceClockGallery
      currentDistance="1700"
      results={[
        result({ kaisaiTsukihi: "0322", sohaTime: "1050" }),
        result({ kaisaiTsukihi: "0101", sohaTime: "1100" }),
      ]}
      runners={[]}
      stats={null}
    />,
  );
  expect(document.querySelectorAll("circle.training-chart-point").length).toBe(2);
});

it("shows a tooltip when hovering a same-condition reference line", () => {
  render(
    <HorseRaceClockGallery
      currentDistance="1700"
      results={[result({})]}
      runners={[]}
      stats={stats()}
    />,
  );
  const hit = document.querySelector('[data-reference-hit="fastestTime"]');
  const visible = document.querySelector('[data-reference="fastestTime"]');
  if (hit === null || visible === null) {
    throw new Error("expected a reference hit target");
  }
  fireEvent.pointerEnter(hit, { clientX: 40, clientY: 48 });
  expect(screen.getByRole("tooltip").textContent).toBe("最速レースタイム 2:23.6");
  expect(visible.getAttribute("stroke-opacity")).toBe("0.7");
  fireEvent.pointerMove(hit, { clientX: 48, clientY: 52 });
  expect(screen.getByRole("tooltip").textContent).toBe("最速レースタイム 2:23.6");
  fireEvent.pointerLeave(hit);
  expect(screen.queryByRole("tooltip")).toBeNull();
  expect(visible.getAttribute("stroke-opacity")).toBe("0.22");
});

it("dims the other horse while hovering a clock point", () => {
  render(
    <HorseRaceClockGallery
      currentDistance="1700"
      results={[
        result({ bamei: "ホバー馬" }),
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
  const hoverPoint = document.querySelector('[data-horse="ホバー馬"]');
  const otherPoint = document.querySelector('[data-horse="別の馬"]');
  if (hoverPoint === null || otherPoint === null) {
    throw new Error("expected both scatter points");
  }
  fireEvent.pointerEnter(hoverPoint, { clientX: 40, clientY: 48 });
  expect(hoverPoint.getAttribute("r")).toBe("7");
  expect(otherPoint.getAttribute("fill-opacity")).toBe("0.08");
  expect(document.querySelector('[data-umaban-label="1"]')?.getAttribute("opacity")).toBe("1");
  expect(document.querySelector('[data-umaban-label="2"]')?.getAttribute("opacity")).toBe("0.22");
});
