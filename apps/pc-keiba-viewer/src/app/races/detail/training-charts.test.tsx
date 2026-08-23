// This file runs with bun.

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import type { Training } from "../../../lib/race-types";
import { TrainingTimesChart } from "./training-charts";

afterEach(cleanup);

const training = (overrides: Partial<Training>): Training => ({
  babamawari: "1",
  bamei: "チャートホース",
  chokyoJikoku: "0600",
  chokyoNengappi: "20260516",
  course: "1",
  currentJockeyName: "騎手",
  lapTime10f: null,
  lapTime1f: "120",
  lapTime2f: null,
  lapTime3f: null,
  lapTime4f: null,
  lapTime5f: null,
  lapTime6f: null,
  lapTime7f: null,
  lapTime8f: null,
  lapTime9f: null,
  premiumEvaluationGrade: "A",
  premiumEvaluationText: "動き良い",
  timeGokei10f: null,
  timeGokei2f: "240",
  timeGokei3f: "360",
  timeGokei4f: "480",
  timeGokei5f: null,
  timeGokei6f: null,
  timeGokei7f: null,
  timeGokei8f: null,
  timeGokei9f: null,
  tracenKubun: "1",
  trainerName: "調教師",
  trainingRiderName: "騎乗者",
  trainingType: "坂路",
  umaban: "01",
  ...overrides,
});

it("renders one last-1F scatter instead of course-faceted raw clocks", () => {
  render(
    <TrainingTimesChart
      showAllWorkouts={false}
      trainings={[training({})]}
      onShowAllWorkouts={() => undefined}
    />,
  );
  expect(screen.getByRole("figure", { name: "調教追い切り散布図" })).toBeDefined();
  expect(
    screen.getByText(
      "点線の十字は均等ペースです。上ほど最終1Fが自分の均等より速く、右ほど最終2Fが自分の均等より速い。2Fがない調教は3Fで見ます。コースが違う時計は直接比べません。点の色は調教記号、数字は馬番。コースはホバーで確認。",
    ),
  ).toBeDefined();
  expect(screen.getByText("最終2F（右が均等より速い）")).toBeDefined();
  expect(screen.getByText("最終1F（上が均等より速い）")).toBeDefined();
  expect(screen.getByText("均等ペース")).toBeDefined();
  expect(screen.getByRole("radio", { name: "最新1本" })).toHaveProperty("checked", true);
  expect(screen.getByRole("radio", { name: "推移" })).toHaveProperty("checked", false);
  expect(screen.queryByRole("region", { name: "坂路の追い切り" })).toBeNull();
  expect(document.querySelectorAll("circle.training-chart-point").length).toBe(1);
  expect(document.querySelector("circle.training-chart-point")?.getAttribute("r")).toBe("4.6");
  expect(document.querySelector("circle.training-chart-point")?.getAttribute("stroke-width")).toBe(
    "1.8",
  );
  expect(document.querySelector("circle.training-chart-point")?.getAttribute("fill-opacity")).toBe(
    "0.5",
  );
  expect(
    document.querySelector("circle.training-chart-point")?.getAttribute("stroke-opacity"),
  ).toBe("0.95");
  expect(document.querySelector('[data-umaban-label="1"]')?.getAttribute("font-size")).toBe("13");
  expect(document.querySelector('[data-umaban-label="1"]')?.getAttribute("font-weight")).toBe(
    "700",
  );
  expect(document.querySelector('[data-umaban-label="1"]')?.getAttribute("opacity")).toBe("0.95");
  expect(document.querySelectorAll("line.training-chart-even").length).toBe(2);
  expect(document.querySelectorAll("path[data-umaban]").length).toBe(0);
});

it("shows an empty message when no last-1F points can be plotted", () => {
  render(
    <TrainingTimesChart
      showAllWorkouts={false}
      trainings={[
        training({
          lapTime1f: "000",
          timeGokei3f: null,
          timeGokei4f: null,
        }),
      ]}
      onShowAllWorkouts={() => undefined}
    />,
  );
  expect(screen.getByText("調教グラフに使えるタイムがありません。")).toBeDefined();
  expect(screen.queryByRole("figure", { name: "調教追い切り散布図" })).toBeNull();
});

it("follows the pointer with a tooltip that includes course, clocks, and splits", () => {
  render(
    <TrainingTimesChart
      showAllWorkouts={false}
      trainings={[
        training({ bamei: "ホバーホース", umaban: "01" }),
        training({
          bamei: "別の馬",
          premiumEvaluationGrade: "C",
          umaban: "02",
        }),
      ]}
      onShowAllWorkouts={() => undefined}
    />,
  );
  const hoverPoint = document.querySelector('[data-horse="ホバーホース"]');
  if (hoverPoint === null) {
    throw new Error("expected the hovered scatter point");
  }
  fireEvent.pointerEnter(hoverPoint, { clientX: 40, clientY: 48 });
  const otherPoint = document.querySelector('[data-horse="別の馬"]');
  if (otherPoint === null) {
    throw new Error("expected the dimmed scatter point");
  }
  expect(hoverPoint.getAttribute("r")).toBe("6.2");
  expect(hoverPoint.getAttribute("stroke-width")).toBe("2.4");
  expect(hoverPoint.getAttribute("fill-opacity")).toBe("0.85");
  expect(otherPoint.getAttribute("r")).toBe("4.6");
  expect(otherPoint.getAttribute("stroke-width")).toBe("1.8");
  expect(otherPoint.getAttribute("fill-opacity")).toBe("0.1");
  expect(otherPoint.getAttribute("stroke-opacity")).toBe("0.14");
  expect(document.querySelector('[data-umaban-label="1"]')?.getAttribute("font-size")).toBe("15");
  expect(document.querySelector('[data-umaban-label="1"]')?.getAttribute("font-weight")).toBe(
    "800",
  );
  expect(document.querySelector('[data-umaban-label="1"]')?.getAttribute("opacity")).toBe("1");
  expect(document.querySelector('[data-umaban-label="2"]')?.getAttribute("font-size")).toBe("13");
  expect(document.querySelector('[data-umaban-label="2"]')?.getAttribute("font-weight")).toBe(
    "700",
  );
  expect(document.querySelector('[data-umaban-label="2"]')?.getAttribute("opacity")).toBe("0.16");
  expect(screen.getByRole("tooltip")).toBeDefined();
  expect(screen.getByText("1 ホバーホース")).toBeDefined();
  expect(screen.getByText("2026-05-16")).toBeDefined();
  expect(screen.getByText("栗東 / 坂路 / Aコース / 外")).toBeDefined();
  expect(screen.getByText("評価 A 動き良い")).toBeDefined();
  expect(screen.getByText("6F -")).toBeDefined();
  expect(screen.getByText("5F -")).toBeDefined();
  expect(screen.getByText("4F 48.0")).toBeDefined();
  expect(screen.getByText("3F 36.0")).toBeDefined();
  expect(screen.getByText("2F 24.0")).toBeDefined();
  expect(screen.getByText("1F 12.0")).toBeDefined();
  expect(screen.getByText("3-2 12.0")).toBeDefined();
  expect(screen.getByText("2-1 12.0")).toBeDefined();
  expect(screen.getByText("最終1F 1.00（均等）")).toBeDefined();
  expect(screen.getByText("最終2F 1.00（均等）")).toBeDefined();
  expect(screen.getByRole("tooltip").style.left).toBe("52px");
  expect(screen.getByRole("tooltip").style.top).toBe("60px");
  fireEvent.pointerMove(hoverPoint, { clientX: 80, clientY: 12 });
  expect(screen.getByRole("tooltip").style.left).toBe("92px");
  expect(screen.getByRole("tooltip").style.top).toBe("24px");
  fireEvent.pointerLeave(hoverPoint);
  expect(screen.queryByRole("tooltip")).toBeNull();
});

it("renders 坂路 and ウッド horses on the same scatter", () => {
  render(
    <TrainingTimesChart
      showAllWorkouts={false}
      trainings={[
        training({
          bamei: "坂路馬",
          trainingType: "坂路",
          umaban: "01",
        }),
        training({
          bamei: "ウッド馬",
          premiumEvaluationGrade: "C",
          trainingType: "ウッド",
          umaban: "02",
        }),
      ]}
      onShowAllWorkouts={() => undefined}
    />,
  );
  expect(screen.queryByRole("region", { name: "坂路の追い切り" })).toBeNull();
  expect(screen.queryByRole("region", { name: "ウッドの追い切り" })).toBeNull();
  expect(document.querySelectorAll("circle.training-chart-point").length).toBe(2);
  expect(document.querySelectorAll('[data-umaban-label="1"]').length).toBe(1);
  expect(document.querySelectorAll('[data-umaban-label="2"]').length).toBe(1);
  expect(document.querySelectorAll('[data-course-facet="坂路"]').length).toBe(1);
  expect(document.querySelectorAll('[data-course-facet="ウッド"]').length).toBe(1);
});

it("keeps one point per horse until 推移 is selected", () => {
  const onShowAllWorkouts = vi.fn<(showAllWorkouts: boolean) => void>();
  render(
    <TrainingTimesChart
      showAllWorkouts={false}
      trainings={[
        training({
          bamei: "複数調教馬",
          chokyoNengappi: "20260510",
          lapTime1f: "130",
          timeGokei4f: "500",
          trainingType: "坂路",
          umaban: "01",
        }),
        training({
          bamei: "複数調教馬",
          chokyoNengappi: "20260516",
          lapTime1f: "120",
          timeGokei4f: "480",
          trainingType: "ウッド",
          umaban: "01",
        }),
      ]}
      onShowAllWorkouts={onShowAllWorkouts}
    />,
  );
  expect(document.querySelectorAll("circle.training-chart-point").length).toBe(1);
  expect(document.querySelectorAll('[data-umaban-label="1"]').length).toBe(1);
  expect(document.querySelectorAll("[data-trend-lane]").length).toBe(0);
  fireEvent.click(screen.getByRole("radio", { name: "推移" }));
  expect(onShowAllWorkouts.mock.calls).toStrictEqual([[true]]);
});

it("plots every workout of a horse on a pale trend lane", () => {
  const onShowAllWorkouts = vi.fn<(showAllWorkouts: boolean) => void>();
  render(
    <TrainingTimesChart
      showAllWorkouts={true}
      trainings={[
        training({
          bamei: "複数調教馬",
          chokyoNengappi: "20260510",
          lapTime1f: "130",
          timeGokei4f: "500",
          trainingType: "坂路",
          umaban: "01",
        }),
        training({
          bamei: "複数調教馬",
          chokyoNengappi: "20260516",
          lapTime1f: "120",
          timeGokei4f: "480",
          trainingType: "ウッド",
          umaban: "01",
        }),
      ]}
      onShowAllWorkouts={onShowAllWorkouts}
    />,
  );
  expect(screen.getByRole("figure", { name: "調教追い切り推移" })).toBeDefined();
  expect(document.querySelectorAll("circle.training-chart-point").length).toBe(2);
  expect(document.querySelectorAll('[data-trend-lane="1"]').length).toBe(1);
  expect(document.querySelectorAll('[data-trend-umaban="1"]').length).toBe(1);
  expect(document.querySelectorAll('[data-trend-path="1"]').length).toBe(1);
  expect(document.querySelector('[data-trend-path="1"]')?.getAttribute("stroke")).toBe("#d5ddd8");
  expect(document.querySelector('[data-trend-path="1"]')?.getAttribute("opacity")).toBe("0.32");
  expect(document.querySelectorAll('[data-umaban-label="1"]').length).toBe(0);
  expect(document.querySelectorAll('[data-latest="1"]').length).toBe(1);
  expect(document.querySelectorAll('[data-latest="0"]').length).toBe(1);
  expect(document.querySelector('circle[data-latest="1"]')?.getAttribute("r")).toBe("4.6");
  expect(document.querySelector('circle[data-latest="0"]')?.getAttribute("r")).toBe("2.7");
  const latestPoint = document.querySelector('circle[data-latest="1"]');
  if (latestPoint === null) {
    throw new Error("expected the latest trend point");
  }
  fireEvent.pointerEnter(latestPoint, { clientX: 40, clientY: 48 });
  expect(document.querySelector('circle[data-latest="0"]')?.getAttribute("r")).toBe("6.2");
  expect(document.querySelector('[data-trend-path="1"]')?.getAttribute("opacity")).toBe("0.5");
  fireEvent.click(screen.getByRole("radio", { name: "最新1本" }));
  expect(onShowAllWorkouts.mock.calls).toStrictEqual([[false]]);
});

it("dims older and latest marks of a non-hovered horse separately", () => {
  render(
    <TrainingTimesChart
      showAllWorkouts={true}
      trainings={[
        training({
          bamei: "複数調教馬",
          chokyoNengappi: "20260510",
          lapTime1f: "130",
          timeGokei4f: "500",
          trainingType: "坂路",
          umaban: "01",
        }),
        training({
          bamei: "複数調教馬",
          chokyoNengappi: "20260516",
          lapTime1f: "120",
          timeGokei4f: "480",
          trainingType: "ウッド",
          umaban: "01",
        }),
        training({
          bamei: "別の馬",
          premiumEvaluationGrade: "C",
          umaban: "02",
        }),
      ]}
      onShowAllWorkouts={() => undefined}
    />,
  );
  const otherPoint = document.querySelector('[data-horse="別の馬"]');
  if (otherPoint === null) {
    throw new Error("expected the hovered scatter point");
  }
  fireEvent.pointerEnter(otherPoint, { clientX: 40, clientY: 48 });
  expect(document.querySelector('circle[data-latest="0"]')?.getAttribute("r")).toBe("2.7");
  expect(document.querySelector('circle[data-latest="0"]')?.getAttribute("stroke-width")).toBe(
    "0.7",
  );
  expect(document.querySelector('circle[data-latest="0"]')?.getAttribute("fill-opacity")).toBe(
    "0.1",
  );
  expect(
    document.querySelector('circle[data-latest="1"][data-umaban="1"]')?.getAttribute("r"),
  ).toBe("4.6");
  expect(
    document
      .querySelector('circle[data-latest="1"][data-umaban="1"]')
      ?.getAttribute("stroke-width"),
  ).toBe("1.8");
  expect(
    document
      .querySelector('circle[data-latest="1"][data-umaban="1"]')
      ?.getAttribute("fill-opacity"),
  ).toBe("0.1");
  expect(document.querySelector('[data-trend-umaban="1"]')?.getAttribute("opacity")).toBe("0.16");
  expect(document.querySelector('[data-trend-path="1"]')?.getAttribute("opacity")).toBe("0.08");
});

it("falls back to 3F even pace when 4F is missing", () => {
  render(
    <TrainingTimesChart
      showAllWorkouts={false}
      trainings={[
        training({
          timeGokei4f: null,
        }),
      ]}
      onShowAllWorkouts={() => undefined}
    />,
  );
  expect(document.querySelector('[data-even-furlongs="3"]') === null).toBe(false);
});

it("renders grade legend swatches", () => {
  render(
    <TrainingTimesChart
      showAllWorkouts={false}
      trainings={[training({})]}
      onShowAllWorkouts={() => undefined}
    />,
  );
  expect(screen.getByText("◎ / S / 1")).toBeDefined();
  expect(screen.getByText("○ / A / 2")).toBeDefined();
  expect(screen.getByText("▲ / B / 3")).toBeDefined();
  expect(screen.getByText("△ / C / 4")).toBeDefined();
  expect(screen.getByText("記号なし")).toBeDefined();
});
