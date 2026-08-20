// bun で実行する (bunx vitest)
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";

import type {
  BloodlineStatsRow,
  FrameStatsRow,
  Runner,
  SimilarRaceStatsRow,
} from "../../../lib/race-types";
import { WinRateHeatmapSection } from "./win-rate-heatmap-section";

afterEach(() => {
  cleanup();
});

const runner: Runner = {
  banushimei: "Owner A",
  barei: "4",
  bamei: "Alpha",
  bataiju: null,
  chokyoshimeiRyakusho: "Trainer A",
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  damSireName: null,
  futanJuryo: "570",
  kakuteiChakujun: "00",
  kettoTorokuBango: "2020100001",
  kishumeiRyakusho: "Jockey A",
  kohan3f: null,
  seibetsuCode: "1",
  sireName: null,
  sireSireName: null,
  sohaTime: null,
  tanshoNinkijun: "00",
  tanshoOdds: "0000",
  timeSa: null,
  umaban: "01",
  wakuban: "1",
  zogenFugo: null,
  zogenSa: null,
};

const similarJockey: SimilarRaceStatsRow = {
  category: "jockey",
  currentHorseNumbers: "1",
  details: [],
  horseCount: 40,
  name: "Jockey A",
  quinellaCount: 20,
  quinellaRate: 25,
  showCount: 30,
  showRate: 37.5,
  starts: 80,
  winCount: 16,
  winRate: 20,
};

const similarTrainer: SimilarRaceStatsRow = {
  category: "trainer",
  currentHorseNumbers: "1",
  details: [],
  horseCount: 12,
  name: "Trainer A",
  quinellaCount: 8,
  quinellaRate: 16,
  showCount: 10,
  showRate: 20,
  starts: 50,
  winCount: 5,
  winRate: 10,
};

const bloodlineSire: BloodlineStatsRow = {
  category: "sire",
  currentHorseNumbers: "1",
  details: [],
  horseCount: 30,
  name: "Sire Alpha",
  quinellaCount: 40,
  quinellaRate: 20,
  showCount: 60,
  showRate: 30,
  starts: 200,
  winCount: 24,
  winRate: 12,
};

const frameOne: FrameStatsRow = {
  averageFinish: 3.2,
  averagePopularity: 4.1,
  count: 40,
  details: [],
  frameNumber: "1",
  medianFinish: 2.5,
  medianPopularity: 3,
  quinellaCount: 12,
  quinellaRate: 30,
  runnerCount: 16,
  score: 0.8,
  showCount: 18,
  showRate: 45,
  winCount: 6,
  winRate: 15,
};

it("shows an empty state when there are no runners", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[]}
      horseResults={[]}
      runners={[]}
      similarRows={[]}
    />,
  );
  expect(screen.getByText("勝率ヒートマップを表示する出走馬がありません。")).toBeDefined();
});

it("renders a heatmap of win rates by default without a horse-name column", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[bloodlineSire]}
      frameStats={[frameOne]}
      horseResults={[]}
      runners={[runner]}
      similarRows={[similarJockey, similarTrainer]}
    />,
  );
  expect(screen.queryByText("馬名")).toBeNull();
  expect(screen.getByText("番")).toBeDefined();
  expect(screen.getByText("枠")).toBeDefined();
  expect(screen.getByText("馬")).toBeDefined();
  expect(screen.getByText("騎手")).toBeDefined();
  expect(screen.getByText("調教師")).toBeDefined();
  expect(screen.getByText("父")).toBeDefined();
  expect(screen.getByText("母父")).toBeDefined();
  expect(screen.getByText("父父")).toBeDefined();
  expect(screen.getAllByText("勝").length).toBe(7);
  expect(screen.queryByText("連")).toBeNull();
  expect(screen.queryByText("複")).toBeNull();
  expect(screen.getByText("20.0%")).toBeDefined();
  expect(screen.getByText("10.0%")).toBeDefined();
  expect(screen.getByText("12.0%")).toBeDefined();
  expect(screen.getByText("15.0%")).toBeDefined();
  expect(screen.queryByText("25.0%")).toBeNull();
  expect(screen.queryByText("37.5%")).toBeNull();
  expect(screen.queryByText("16.0%")).toBeNull();
  expect(screen.queryByText("30.0%")).toBeNull();
  expect(screen.queryByText("45.0%")).toBeNull();
  expect(screen.getByTitle("Alpha")).toBeDefined();
  expect(screen.getByText("Alpha")).toBeDefined();
  expect(screen.getByText("Jockey A")).toBeDefined();
  expect(screen.getByText("Trainer A")).toBeDefined();
  expect(
    document.querySelector(".win-rate-heatmap-tooltip .frame-number-badge.frame-1"),
  ).toBeDefined();
  expect(screen.queryByTitle("枠の勝率: 枠1 15.0%（40走）")).toBeNull();
  expect(
    screen.queryByText(
      "番だけで馬を識別します。枠は同条件レース分析の枠番成績です。勝・連・複は色の濃さのヒートマップです。赤が勝率、橙が連対率、青が複勝率で、濃いほど高いです。",
    ),
  ).toBeNull();
  const tableWrap = document.querySelector(".win-rate-heatmap-table-wrap");
  const viewToggle = document.querySelector(".win-rate-heatmap-view-toggle");
  expect(viewToggle instanceof HTMLFieldSetElement).toBe(true);
  expect(tableWrap instanceof HTMLDivElement).toBe(true);
  expect(tableWrap?.contains(viewToggle)).toBe(false);
  expect(tableWrap?.querySelector(".win-rate-heatmap-table") instanceof HTMLTableElement).toBe(
    true,
  );
  expect(document.querySelector(".running-style-bucket-controls")).toBeNull();
  expect(screen.getByRole("radio", { name: /^勝率$/ })).toHaveProperty("checked", true);
  expect(screen.getByRole("radio", { name: /^連対率$/ })).toHaveProperty("checked", false);
  expect(screen.getByRole("radio", { name: /^複勝率$/ })).toHaveProperty("checked", false);
  expect(screen.getByRole("radio", { name: "勝率+連対率+複勝率" })).toHaveProperty(
    "checked",
    false,
  );
});

it("shows quinella-rate swatches when the quinella-rate radio is selected", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[bloodlineSire]}
      frameStats={[frameOne]}
      horseResults={[]}
      runners={[runner]}
      similarRows={[similarJockey, similarTrainer]}
    />,
  );
  fireEvent.click(screen.getByRole("radio", { name: /^連対率$/ }));
  expect(screen.getByRole("radio", { name: /^連対率$/ })).toHaveProperty("checked", true);
  expect(screen.getByRole("radio", { name: /^勝率$/ })).toHaveProperty("checked", false);
  expect(screen.getByRole("radio", { name: /^複勝率$/ })).toHaveProperty("checked", false);
  expect(screen.getByRole("radio", { name: "勝率+連対率+複勝率" })).toHaveProperty(
    "checked",
    false,
  );
  expect(screen.getAllByText("連").length).toBe(7);
  expect(screen.queryByText("勝")).toBeNull();
  expect(screen.queryByText("複")).toBeNull();
  expect(screen.getByText("30.0%")).toBeDefined();
  expect(screen.queryByText("15.0%")).toBeNull();
  expect(screen.queryByText("45.0%")).toBeNull();
});

it("shows show-rate swatches when the show-rate radio is selected", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[bloodlineSire]}
      frameStats={[frameOne]}
      horseResults={[]}
      runners={[runner]}
      similarRows={[similarJockey, similarTrainer]}
    />,
  );
  fireEvent.click(screen.getByRole("radio", { name: /^複勝率$/ }));
  expect(screen.getByRole("radio", { name: /^複勝率$/ })).toHaveProperty("checked", true);
  expect(screen.getByRole("radio", { name: /^勝率$/ })).toHaveProperty("checked", false);
  expect(screen.getByRole("radio", { name: /^連対率$/ })).toHaveProperty("checked", false);
  expect(screen.getByRole("radio", { name: "勝率+連対率+複勝率" })).toHaveProperty(
    "checked",
    false,
  );
  expect(screen.getAllByText("複").length).toBe(7);
  expect(screen.queryByText("勝")).toBeNull();
  expect(screen.queryByText("連")).toBeNull();
  expect(screen.getByText("45.0%")).toBeDefined();
  expect(screen.queryByText("15.0%")).toBeNull();
});

it("shows win, quinella, and show swatches when the combined radio is selected", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[bloodlineSire]}
      frameStats={[frameOne]}
      horseResults={[]}
      runners={[runner]}
      similarRows={[similarJockey, similarTrainer]}
    />,
  );
  fireEvent.click(screen.getByRole("radio", { name: "勝率+連対率+複勝率" }));
  expect(screen.getByRole("radio", { name: "勝率+連対率+複勝率" })).toHaveProperty("checked", true);
  expect(screen.getByRole("radio", { name: /^勝率$/ })).toHaveProperty("checked", false);
  expect(screen.getByRole("radio", { name: /^連対率$/ })).toHaveProperty("checked", false);
  expect(screen.getByRole("radio", { name: /^複勝率$/ })).toHaveProperty("checked", false);
  expect(screen.getAllByText("勝").length).toBe(7);
  expect(screen.getAllByText("連").length).toBe(7);
  expect(screen.getAllByText("複").length).toBe(7);
  expect(screen.getByText("15.0%")).toBeDefined();
  expect(screen.getAllByText("30.0%").length).toBe(2);
  expect(screen.getByText("45.0%")).toBeDefined();
});

it("shows computed frame win rate when the payload omits rate fields but includes counts", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[{ ...frameOne, winRate: Number.NaN }]}
      horseResults={[]}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  expect(screen.getByText("15.0%")).toBeDefined();
});

it("renders a dash instead of throwing when frame win rate is not a finite number", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[
        {
          ...frameOne,
          count: Number.NaN,
          quinellaCount: Number.NaN,
          quinellaRate: Number.NaN,
          showCount: Number.NaN,
          showRate: Number.NaN,
          winCount: Number.NaN,
          winRate: Number.NaN,
        },
      ]}
      horseResults={[]}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  expect(
    document.querySelector(".win-rate-heatmap-tooltip .frame-number-badge.frame-1"),
  ).toBeDefined();
});

it("shows missing frame rates as dashes when no matching frame row exists", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[]}
      horseResults={[]}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  expect(
    document.querySelector(".win-rate-heatmap-tooltip .frame-number-badge.frame-1"),
  ).toBeDefined();
  expect(screen.getAllByRole("tooltip").length).toBe(7);
});

it("opens a heatmap tooltip on click and closes it on a second click", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[frameOne]}
      horseResults={[]}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  const swatch = document.querySelector(".win-rate-heatmap-swatch");
  const button = document.querySelector(".win-rate-heatmap-swatch-button");
  if (!(swatch instanceof HTMLTableCellElement)) {
    throw new Error("expected heatmap swatch");
  }
  if (!(button instanceof HTMLButtonElement)) {
    throw new Error("expected heatmap swatch button");
  }
  fireEvent.click(button);
  expect(swatch.className).toBe("win-rate-heatmap-swatch tooltip-open");
  fireEvent.click(button);
  expect(swatch.className).toBe("win-rate-heatmap-swatch");
});

it("moves the open heatmap tooltip to another cell on click", () => {
  render(
    <WinRateHeatmapSection
      bloodlineRows={[]}
      frameStats={[frameOne]}
      horseResults={[]}
      runners={[runner]}
      similarRows={[]}
    />,
  );
  const swatches = document.querySelectorAll(".win-rate-heatmap-swatch");
  const buttons = document.querySelectorAll(".win-rate-heatmap-swatch-button");
  const firstSwatch = swatches[0];
  const secondSwatch = swatches[1];
  const firstButton = buttons[0];
  const secondButton = buttons[1];
  if (!(firstSwatch instanceof HTMLTableCellElement)) {
    throw new Error("expected first heatmap swatch");
  }
  if (!(secondSwatch instanceof HTMLTableCellElement)) {
    throw new Error("expected second heatmap swatch");
  }
  if (!(firstButton instanceof HTMLButtonElement)) {
    throw new Error("expected first heatmap swatch button");
  }
  if (!(secondButton instanceof HTMLButtonElement)) {
    throw new Error("expected second heatmap swatch button");
  }
  fireEvent.click(firstButton);
  expect(firstSwatch.className).toBe("win-rate-heatmap-swatch tooltip-open");
  fireEvent.click(secondButton);
  expect(firstSwatch.className).toBe("win-rate-heatmap-swatch");
  expect(secondSwatch.className).toBe("win-rate-heatmap-swatch tooltip-open");
});
