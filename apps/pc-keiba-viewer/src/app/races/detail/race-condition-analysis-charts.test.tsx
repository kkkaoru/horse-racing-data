// This file runs with bun.

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";

import { FinishPopularityChart, PayoutTrendChart } from "./race-condition-analysis-charts";

afterEach(cleanup);

it("renders a D3 payout boxplot instead of grouped bars", () => {
  render(
    <PayoutTrendChart
      view={{
        bees: [
          {
            betType: "単勝",
            date: "20260101",
            index: 0,
            isOutlier: false,
            raceName: "A",
            yen: 200,
          },
          {
            betType: "単勝",
            date: "20260102",
            index: 1,
            isOutlier: false,
            raceName: "B",
            yen: 300,
          },
          {
            betType: "単勝",
            date: "20260103",
            index: 2,
            isOutlier: false,
            raceName: "C",
            yen: 400,
          },
          {
            betType: "単勝",
            date: "20260104",
            index: 3,
            isOutlier: false,
            raceName: "D",
            yen: 500,
          },
          {
            betType: "単勝",
            date: "20260105",
            index: 4,
            isOutlier: true,
            raceName: "E",
            yen: 8000,
          },
        ],
        boxes: [
          {
            betType: "単勝",
            count: 5,
            median: 400,
            q1: 300,
            q3: 500,
            samples: [200, 300, 400, 500, 8000],
            whiskerHigh: 500,
            whiskerLow: 200,
          },
        ],
        ranges: [],
      }}
    />,
  );
  expect(screen.getByRole("figure", { name: "払い戻し傾向グラフ" })).toBeDefined();
  expect(screen.getByLabelText("払い戻し箱ひげ図")).toBeDefined();
  expect(screen.getByText("単勝")).toBeDefined();
  expect(screen.getByText("300円")).toBeDefined();
  expect(screen.getByText("1,000円")).toBeDefined();
  expect(screen.getByText("3,000円")).toBeDefined();
  expect(
    screen.getByText(
      "箱は四分位、ひげは外れ値を除いた範囲、点は各レースの払戻です。縦軸は対数なので、単勝から三連単まで分布の形と大穴を同じ図で比較できます。",
    ),
  ).toBeDefined();
  expect(document.querySelectorAll('[data-chart-point="payout-bee"]').length).toBe(5);
  expect(document.querySelectorAll('[data-chart-shape="box"]').length).toBe(1);
});

it("follows the pointer with a payout tooltip that includes yen and race detail", () => {
  render(
    <PayoutTrendChart
      view={{
        bees: [
          {
            betType: "単勝",
            date: "20260101",
            index: 0,
            isOutlier: false,
            raceName: "A",
            yen: 200,
          },
          {
            betType: "単勝",
            date: "20260102",
            index: 1,
            isOutlier: false,
            raceName: "B",
            yen: 300,
          },
          {
            betType: "単勝",
            date: "20260103",
            index: 2,
            isOutlier: false,
            raceName: "C",
            yen: 400,
          },
          {
            betType: "単勝",
            date: "20260104",
            index: 3,
            isOutlier: false,
            raceName: "D",
            yen: 500,
          },
          {
            betType: "単勝",
            date: "20260105",
            index: 4,
            isOutlier: true,
            raceName: "E",
            yen: 8000,
          },
        ],
        boxes: [
          {
            betType: "単勝",
            count: 5,
            median: 400,
            q1: 300,
            q3: 500,
            samples: [200, 300, 400, 500, 8000],
            whiskerHigh: 500,
            whiskerLow: 200,
          },
        ],
        ranges: [],
      }}
    />,
  );
  const bee = document.querySelector('[data-chart-point="payout-bee"][data-outlier="false"]');
  expect(bee === null).toBe(false);
  if (bee === null) {
    return;
  }
  fireEvent.pointerOver(bee, { clientX: 40, clientY: 48 });
  const tooltip = screen.getByRole("tooltip");
  expect(tooltip.querySelector(".condition-analysis-chart-tooltip-title")?.textContent).toBe(
    "単勝",
  );
  expect(tooltip.querySelector("p:nth-of-type(2)")?.textContent).toBe("200円");
  expect(tooltip.querySelector("p:nth-of-type(3)")?.textContent).toBe("箱ひげ内");
  expect(tooltip.querySelector("p:nth-of-type(4)")?.textContent).toBe(
    "Q1 300円 / 中央値 400円 / Q3 500円",
  );
  expect(tooltip.querySelector(".condition-analysis-chart-tooltip-meta")?.textContent).toBe(
    "A / 2026/01/01",
  );
  fireEvent.pointerLeave(bee);
  expect(screen.queryByRole("tooltip")).toBeNull();
});

it("shows an outlier payout tooltip when hovering the high bee", () => {
  render(
    <PayoutTrendChart
      view={{
        bees: [
          {
            betType: "単勝",
            date: "20260101",
            index: 0,
            isOutlier: false,
            raceName: "A",
            yen: 200,
          },
          {
            betType: "単勝",
            date: "20260102",
            index: 1,
            isOutlier: false,
            raceName: "B",
            yen: 300,
          },
          {
            betType: "単勝",
            date: "20260103",
            index: 2,
            isOutlier: false,
            raceName: "C",
            yen: 400,
          },
          {
            betType: "単勝",
            date: "20260104",
            index: 3,
            isOutlier: false,
            raceName: "D",
            yen: 500,
          },
          {
            betType: "単勝",
            date: "20260105",
            index: 4,
            isOutlier: true,
            raceName: "E",
            yen: 8000,
          },
        ],
        boxes: [
          {
            betType: "単勝",
            count: 5,
            median: 400,
            q1: 300,
            q3: 500,
            samples: [200, 300, 400, 500, 8000],
            whiskerHigh: 500,
            whiskerLow: 200,
          },
        ],
        ranges: [],
      }}
    />,
  );
  const bee = document.querySelector('[data-chart-point="payout-bee"][data-outlier="true"]');
  expect(bee === null).toBe(false);
  if (bee === null) {
    return;
  }
  fireEvent.mouseOver(bee, { clientX: 80, clientY: 12 });
  const tooltip = screen.getByRole("tooltip");
  expect(tooltip.querySelector("p:nth-of-type(2)")?.textContent).toBe("8,000円");
  expect(tooltip.querySelector("p:nth-of-type(3)")?.textContent).toBe("外れ値");
  expect(tooltip.querySelector(".condition-analysis-chart-tooltip-meta")?.textContent).toBe(
    "E / 2026/01/05",
  );
});

it("shows an empty payout chart message when there are no chart rows", () => {
  render(<PayoutTrendChart view={{ bees: [], boxes: [], ranges: [] }} />);
  expect(screen.getByText("払い戻しのグラフに使えるデータがありません。")).toBeDefined();
  expect(screen.queryByRole("figure", { name: "払い戻し傾向グラフ" })).toBeNull();
});

it("renders a single bet type on the D3 band axis", () => {
  render(
    <PayoutTrendChart
      view={{
        bees: [
          {
            betType: "三連単",
            date: "20260101",
            index: 0,
            isOutlier: false,
            raceName: "A",
            yen: 500,
          },
          {
            betType: "三連単",
            date: "20260102",
            index: 1,
            isOutlier: false,
            raceName: "B",
            yen: 500,
          },
          {
            betType: "三連単",
            date: "20260103",
            index: 2,
            isOutlier: false,
            raceName: "C",
            yen: 500,
          },
          {
            betType: "三連単",
            date: "20260104",
            index: 3,
            isOutlier: false,
            raceName: "D",
            yen: 500,
          },
        ],
        boxes: [
          {
            betType: "三連単",
            count: 4,
            median: 500,
            q1: 500,
            q3: 500,
            samples: [500, 500, 500, 500],
            whiskerHigh: 500,
            whiskerLow: 500,
          },
        ],
        ranges: [],
      }}
    />,
  );
  expect(screen.getByText("三連単")).toBeDefined();
  expect(screen.queryByText("単勝")).toBeNull();
  expect(screen.getByText("417円")).toBeDefined();
  expect(screen.getByText("600円")).toBeDefined();
});

it("renders a summary range when Tukey samples are missing", () => {
  render(
    <PayoutTrendChart
      view={{
        bees: [],
        boxes: [],
        ranges: [
          {
            average: 800,
            betType: "馬連",
            count: 10,
            max: 2000,
            median: 500,
            min: 200,
          },
        ],
      }}
    />,
  );
  expect(screen.getByText("馬連")).toBeDefined();
  expect(document.querySelectorAll(".condition-analysis-chart-average").length).toBe(1);
  expect(document.querySelectorAll(".condition-analysis-chart-range").length).toBe(1);
});

it("omits the average marker when a payout range has no average", () => {
  render(
    <PayoutTrendChart
      view={{
        bees: [],
        boxes: [],
        ranges: [
          {
            average: null,
            betType: "枠連",
            count: 4,
            max: 1200,
            median: 400,
            min: 200,
          },
        ],
      }}
    />,
  );
  expect(screen.getByText("枠連")).toBeDefined();
  expect(document.querySelectorAll(".condition-analysis-chart-average").length).toBe(0);
});

it("renders a finish odds boxplot grouped by 着順 instead of a scatter", () => {
  render(
    <FinishPopularityChart
      points={[
        {
          date: "20260111",
          finishGroup: "1着",
          finishPosition: 1,
          horseName: "A",
          odds: 1.5,
          popularity: 1,
          raceName: "一般",
        },
        {
          date: "20260111",
          finishGroup: "1着",
          finishPosition: 1,
          horseName: "B",
          odds: 2,
          popularity: 2,
          raceName: "一般",
        },
        {
          date: "20260111",
          finishGroup: "1着",
          finishPosition: 1,
          horseName: "C",
          odds: 2.5,
          popularity: 3,
          raceName: "一般",
        },
        {
          date: "20260111",
          finishGroup: "1着",
          finishPosition: 1,
          horseName: "D",
          odds: 3,
          popularity: 4,
          raceName: "一般",
        },
        {
          date: "20260112",
          finishGroup: "1着",
          finishPosition: 1,
          horseName: "Hole",
          odds: 80,
          popularity: 12,
          raceName: "特別",
        },
        {
          date: "20260111",
          finishGroup: "2着",
          finishPosition: 2,
          horseName: "Place Horse",
          odds: 4,
          popularity: 2,
          raceName: "一般",
        },
        {
          date: "20260111",
          finishGroup: "3着",
          finishPosition: 3,
          horseName: "Show Horse",
          odds: 8,
          popularity: 4,
          raceName: "一般",
        },
        {
          date: "20260111",
          finishGroup: "着外",
          finishPosition: 8,
          horseName: "Rest Horse",
          odds: 21,
          popularity: 10,
          raceName: "一般",
        },
      ]}
    />,
  );
  expect(screen.getByRole("figure", { name: "着順別人気オッズグラフ" })).toBeDefined();
  expect(screen.getByLabelText("着順別単勝オッズ箱ひげ図")).toBeDefined();
  expect(screen.queryByLabelText("n番人気の勝率連対率複勝率")).toBeNull();
  expect(screen.queryByLabelText("着順別人気散布図")).toBeNull();
  expect(screen.queryByRole("list", { name: "人気成績" })).toBeNull();
  expect(screen.getAllByText("1着").length).toBe(1);
  expect(screen.getAllByText("2着").length).toBe(1);
  expect(screen.getAllByText("3着").length).toBe(1);
  expect(screen.getAllByText("着外").length).toBe(1);
  expect(screen.getByText("2")).toBeDefined();
  expect(screen.getByText("10")).toBeDefined();
  expect(screen.getByText("1.5")).toBeDefined();
  expect(screen.getByText("50")).toBeDefined();
  expect(
    screen.getByText(
      "箱は着順グループごとの単勝オッズの四分位、ひげは外れ値を除いた範囲、点は各馬です。縦軸は対数なので本命と大穴の分布を同じ図で比べられます。",
    ),
  ).toBeDefined();
  expect(document.querySelectorAll('[data-chart-point="finish-bee"]').length).toBe(8);
  expect(document.querySelectorAll('[data-finish-group="1着"]').length).toBe(5);
  expect(document.querySelectorAll('[data-finish-group="2着"]').length).toBe(1);
  expect(document.querySelectorAll('[data-finish-group="3着"]').length).toBe(1);
  expect(document.querySelectorAll('[data-finish-group="着外"]').length).toBe(1);
  expect(document.querySelectorAll('[data-chart-shape="box"]').length).toBe(1);
  expect(document.querySelectorAll('[data-chart-point="popularity-rate"]').length).toBe(0);
});

it("follows the pointer with a finish bee tooltip that includes quartiles", () => {
  render(
    <FinishPopularityChart
      points={[
        {
          date: "20260111",
          finishGroup: "1着",
          finishPosition: 1,
          horseName: "A",
          odds: 1.5,
          popularity: 1,
          raceName: "一般",
        },
        {
          date: "20260111",
          finishGroup: "1着",
          finishPosition: 1,
          horseName: "B",
          odds: 2,
          popularity: 2,
          raceName: "一般",
        },
        {
          date: "20260111",
          finishGroup: "1着",
          finishPosition: 1,
          horseName: "C",
          odds: 2.5,
          popularity: 3,
          raceName: "一般",
        },
        {
          date: "20260111",
          finishGroup: "1着",
          finishPosition: 1,
          horseName: "D",
          odds: 3,
          popularity: 4,
          raceName: "一般",
        },
        {
          date: "20260112",
          finishGroup: "1着",
          finishPosition: 1,
          horseName: "Hole",
          odds: 80,
          popularity: 12,
          raceName: "特別",
        },
      ]}
    />,
  );
  const bee = document.querySelector('[data-horse="Hole"]');
  expect(bee === null).toBe(false);
  if (bee === null) {
    return;
  }
  fireEvent.pointerMove(bee, { clientX: 24, clientY: 16 });
  const tooltip = screen.getByRole("tooltip");
  expect(tooltip.querySelector(".condition-analysis-chart-tooltip-title")?.textContent).toBe(
    "Hole",
  );
  expect(tooltip.querySelector("p:nth-of-type(2)")?.textContent).toBe("1着");
  expect(tooltip.querySelector("p:nth-of-type(3)")?.textContent).toBe("人気 12");
  expect(tooltip.querySelector("p:nth-of-type(4)")?.textContent).toBe("オッズ 80");
  expect(tooltip.querySelector("p:nth-of-type(5)")?.textContent).toBe("外れ値");
  expect(tooltip.querySelector("p:nth-of-type(6)")?.textContent).toBe("Q1 2 / 中央値 2.5 / Q3 3");
  expect(tooltip.querySelector(".condition-analysis-chart-tooltip-meta")?.textContent).toBe(
    "特別 / 2026/01/12",
  );
  fireEvent.pointerLeave(bee);
  expect(screen.queryByRole("tooltip")).toBeNull();
});

it("renders a single finish group and log-padded ticks", () => {
  render(
    <FinishPopularityChart
      points={[
        {
          date: "20260111",
          finishGroup: "1着",
          finishPosition: 1,
          horseName: "Win Horse",
          odds: 2.5,
          popularity: 1,
          raceName: "一般",
        },
      ]}
    />,
  );
  expect(screen.getAllByText("1着").length).toBe(1);
  expect(screen.queryByText("2着")).toBeNull();
  expect(screen.queryByText("3着")).toBeNull();
  expect(screen.queryByText("着外")).toBeNull();
  expect(screen.getByText("3")).toBeDefined();
  expect(document.querySelectorAll('[data-chart-point="finish-bee"]').length).toBe(1);
  expect(document.querySelectorAll('[data-chart-shape="box"]').length).toBe(0);
  expect(document.querySelectorAll('[data-chart-point="popularity-rate"]').length).toBe(0);
  const bee = document.querySelector('[data-horse="Win Horse"]');
  expect(bee === null).toBe(false);
  if (bee === null) {
    return;
  }
  fireEvent.mouseOver(bee, { clientX: 16, clientY: 20 });
  const tooltip = screen.getByRole("tooltip");
  expect(tooltip.querySelector("p:nth-of-type(4)")?.textContent).toBe("オッズ 2.5");
  expect(tooltip.querySelector("p:nth-of-type(5)")?.textContent).toBe("箱ひげ内");
});

it("shows an empty finish chart message when there are no points", () => {
  render(<FinishPopularityChart points={[]} />);
  expect(screen.getByText("着順別のグラフに使えるデータがありません。")).toBeDefined();
  expect(screen.queryByRole("figure", { name: "着順別人気オッズグラフ" })).toBeNull();
});

it("hides the finish chart when every odds value is non-positive", () => {
  render(
    <FinishPopularityChart
      points={[
        {
          date: "20260111",
          finishGroup: "1着",
          finishPosition: 1,
          horseName: "Zero",
          odds: 0,
          popularity: 1,
          raceName: "一般",
        },
      ]}
    />,
  );
  expect(screen.getByText("着順別のグラフに使えるデータがありません。")).toBeDefined();
  expect(screen.queryByRole("figure", { name: "着順別人気オッズグラフ" })).toBeNull();
});

it("draws the finish chart for a single 着外 group with identical odds", () => {
  render(
    <FinishPopularityChart
      points={[
        {
          date: "20260111",
          finishGroup: "着外",
          finishPosition: 8,
          horseName: "Out One",
          odds: 21,
          popularity: 10,
          raceName: "一般",
        },
        {
          date: "20260112",
          finishGroup: "着外",
          finishPosition: 5,
          horseName: "Out Two",
          odds: 21,
          popularity: 7,
          raceName: "特別",
        },
      ]}
    />,
  );
  expect(screen.getByRole("figure", { name: "着順別人気オッズグラフ" })).toBeDefined();
  expect(screen.getAllByText("着外").length).toBe(1);
  expect(screen.queryByText("1着")).toBeNull();
  expect(screen.queryByText("2着")).toBeNull();
  expect(screen.queryByText("3着")).toBeNull();
  expect(document.querySelectorAll('[data-chart-point="finish-bee"]').length).toBe(2);
  expect(document.querySelectorAll('[data-finish-group="着外"]').length).toBe(2);
  expect(document.querySelectorAll('[data-chart-shape="box"]').length).toBe(0);
});
