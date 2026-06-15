// Run with: bunx vitest run src/app/races/detail/horse-race-results-correlation-matrix.test.tsx

import { cleanup, render, screen } from "@testing-library/react";
import React from "react";
import { afterEach, expect, test } from "vitest";

import type { HorseRaceCorrelationRow } from "../../../lib/horse-race-results-chart-data";
import {
  buildWeightSparkline,
  formatDeltaLabel,
  getDeltaBarStyle,
  getMarketGapAppearance,
  getRankBucketColor,
  HorseRaceResultsCorrelationMatrix,
} from "./horse-race-results-correlation-matrix";

afterEach(() => {
  cleanup();
});

const correlationRow = (overrides: Partial<HorseRaceCorrelationRow>): HorseRaceCorrelationRow => ({
  dateValue: Date.UTC(2026, 2, 22),
  finish: 1,
  futan: 56,
  popularity: 2,
  raceDate: "20260322",
  weight: 480,
  weightDelta: 6,
  ...overrides,
});

test("getRankBucketColor returns the gold bucket for rank 1", () => {
  expect(getRankBucketColor(1)).toStrictEqual("#d4a017");
});

test("getRankBucketColor returns the silver bucket for rank 2", () => {
  expect(getRankBucketColor(2)).toStrictEqual("#8e9aa5");
});

test("getRankBucketColor returns the bronze bucket for rank 3", () => {
  expect(getRankBucketColor(3)).toStrictEqual("#a9712d");
});

test("getRankBucketColor returns the blue bucket for ranks 4 and 5", () => {
  expect(getRankBucketColor(4)).toStrictEqual("#5b8db8");
  expect(getRankBucketColor(5)).toStrictEqual("#5b8db8");
});

test("getRankBucketColor returns the gray-blue bucket for ranks 6 and 9", () => {
  expect(getRankBucketColor(6)).toStrictEqual("#8aa0ae");
  expect(getRankBucketColor(9)).toStrictEqual("#8aa0ae");
});

test("getRankBucketColor returns the light bucket for ranks 10 and beyond", () => {
  expect(getRankBucketColor(10)).toStrictEqual("#c9d2d8");
  expect(getRankBucketColor(18)).toStrictEqual("#c9d2d8");
});

test("getRankBucketColor returns the null bucket for a missing rank", () => {
  expect(getRankBucketColor(null)).toStrictEqual("#e5e7eb");
});

test("getMarketGapAppearance flags an over-performance when finish beats popularity", () => {
  expect(getMarketGapAppearance(1, 6)).toStrictEqual({
    className: "race-results-correlation-gap-over",
    label: "-5",
    tone: "over",
  });
});

test("getMarketGapAppearance flags an under-performance when finish trails popularity", () => {
  expect(getMarketGapAppearance(8, 2)).toStrictEqual({
    className: "race-results-correlation-gap-under",
    label: "+6",
    tone: "under",
  });
});

test("getMarketGapAppearance marks an even result when finish equals popularity", () => {
  expect(getMarketGapAppearance(3, 3)).toStrictEqual({
    className: "race-results-correlation-gap-even",
    label: "±0",
    tone: "even",
  });
});

test("getMarketGapAppearance returns a dash when finish is missing", () => {
  expect(getMarketGapAppearance(null, 4)).toStrictEqual({
    className: "race-results-correlation-gap-even",
    label: "-",
    tone: "even",
  });
});

test("getMarketGapAppearance returns a dash when popularity is missing", () => {
  expect(getMarketGapAppearance(4, null)).toStrictEqual({
    className: "race-results-correlation-gap-even",
    label: "-",
    tone: "even",
  });
});

test("formatDeltaLabel prefixes positive deltas with a plus sign", () => {
  expect(formatDeltaLabel(6)).toStrictEqual("+6");
});

test("formatDeltaLabel keeps the minus sign of negative deltas", () => {
  expect(formatDeltaLabel(-4)).toStrictEqual("-4");
});

test("formatDeltaLabel renders zero as plus-minus zero", () => {
  expect(formatDeltaLabel(0)).toStrictEqual("±0");
});

test("formatDeltaLabel renders a dash for a missing delta", () => {
  expect(formatDeltaLabel(null)).toStrictEqual("-");
});

test("getDeltaBarStyle builds an upward orange bar for a positive delta", () => {
  expect(getDeltaBarStyle(6, 6)).toStrictEqual({
    backgroundColor: "#ea580c",
    direction: "up",
    heightPercent: 100,
  });
});

test("getDeltaBarStyle builds a downward blue bar for a negative delta", () => {
  expect(getDeltaBarStyle(-4, 6)).toStrictEqual({
    backgroundColor: "#2563eb",
    direction: "down",
    heightPercent: 67,
  });
});

test("getDeltaBarStyle builds a minimal gray stub for a zero delta", () => {
  expect(getDeltaBarStyle(0, 6)).toStrictEqual({
    backgroundColor: "#9ca3af",
    direction: "up",
    heightPercent: 8,
  });
});

test("getDeltaBarStyle returns null for a missing delta", () => {
  expect(getDeltaBarStyle(null, 6)).toStrictEqual(null);
});

test("getDeltaBarStyle scales against the delta itself when the maximum is smaller", () => {
  expect(getDeltaBarStyle(6, 0)).toStrictEqual({
    backgroundColor: "#ea580c",
    direction: "up",
    heightPercent: 100,
  });
});

test("buildWeightSparkline returns empty geometry when every weight is missing", () => {
  expect(
    buildWeightSparkline({
      columnWidth: 10,
      height: 48,
      rows: [correlationRow({ weight: null })],
    }),
  ).toStrictEqual({ points: [], segments: [] });
});

test("buildWeightSparkline scales two weights into one segment", () => {
  expect(
    buildWeightSparkline({
      columnWidth: 10,
      height: 48,
      rows: [
        correlationRow({ weight: 480 }),
        correlationRow({ raceDate: "20260510", weight: 490 }),
      ],
    }),
  ).toStrictEqual({
    points: [
      { cx: 5, cy: 42 },
      { cx: 15, cy: 6 },
    ],
    segments: ["5,42 15,6"],
  });
});

test("buildWeightSparkline splits segments around a missing weight", () => {
  expect(
    buildWeightSparkline({
      columnWidth: 10,
      height: 48,
      rows: [
        correlationRow({ weight: 480 }),
        correlationRow({ raceDate: "20260405", weight: null }),
        correlationRow({ raceDate: "20260510", weight: 490 }),
      ],
    }),
  ).toStrictEqual({
    points: [
      { cx: 5, cy: 42 },
      { cx: 25, cy: 6 },
    ],
    segments: ["5,42", "25,6"],
  });
});

test("buildWeightSparkline centers the points when every weight is identical", () => {
  expect(
    buildWeightSparkline({
      columnWidth: 10,
      height: 48,
      rows: [
        correlationRow({ weight: 480 }),
        correlationRow({ raceDate: "20260510", weight: 480 }),
      ],
    }),
  ).toStrictEqual({
    points: [
      { cx: 5, cy: 24 },
      { cx: 15, cy: 24 },
    ],
    segments: ["5,24 15,24"],
  });
});

test("buildWeightSparkline ignores a leading missing weight", () => {
  expect(
    buildWeightSparkline({
      columnWidth: 10,
      height: 48,
      rows: [
        correlationRow({ weight: null }),
        correlationRow({ raceDate: "20260510", weight: 480 }),
      ],
    }),
  ).toStrictEqual({
    points: [{ cx: 15, cy: 24 }],
    segments: ["15,24"],
  });
});

test("buildWeightSparkline closes the run before a trailing missing weight", () => {
  expect(
    buildWeightSparkline({
      columnWidth: 10,
      height: 48,
      rows: [
        correlationRow({ weight: 480 }),
        correlationRow({ raceDate: "20260405", weight: 490 }),
        correlationRow({ raceDate: "20260510", weight: null }),
      ],
    }),
  ).toStrictEqual({
    points: [
      { cx: 5, cy: 42 },
      { cx: 15, cy: 6 },
    ],
    segments: ["5,42 15,6"],
  });
});

test("renders the empty guard when the selected horse has no rows", () => {
  render(<HorseRaceResultsCorrelationMatrix rows={[]} />);
  expect(screen.getByText("選択した馬の表示できるデータがありません").textContent).toStrictEqual(
    "選択した馬の表示できるデータがありません",
  );
});

test("renders the row labels and the race-aligned date header", () => {
  const { container } = render(
    <HorseRaceResultsCorrelationMatrix
      rows={[
        correlationRow({}),
        correlationRow({ dateValue: Date.UTC(2026, 4, 10), raceDate: "20260510" }),
      ]}
    />,
  );
  expect(
    Array.from(container.querySelectorAll(".race-results-correlation-row-label")).map(
      (label) => label.textContent,
    ),
  ).toStrictEqual(["日付", "着順", "人気", "差", "馬体重", "", "増減", "斤量"]);
  expect(
    Array.from(container.querySelectorAll(".race-results-correlation-date")).map(
      (cell) => cell.textContent,
    ),
  ).toStrictEqual(["26/03/22", "26/05/10"]);
});

test("sizes the matrix grid with one column per race", () => {
  const { container } = render(
    <HorseRaceResultsCorrelationMatrix
      rows={[
        correlationRow({}),
        correlationRow({ dateValue: Date.UTC(2026, 4, 10), raceDate: "20260510" }),
      ]}
    />,
  );
  const grid = container.querySelector<HTMLElement>(".race-results-correlation-matrix-grid");
  expect(grid?.style.gridTemplateColumns).toStrictEqual("max-content repeat(2, minmax(56px, 1fr))");
});

test("renders finish and popularity badges with the shared bucket colors", () => {
  const { container } = render(
    <HorseRaceResultsCorrelationMatrix
      rows={[
        correlationRow({ finish: 1, popularity: 6 }),
        correlationRow({
          dateValue: Date.UTC(2026, 4, 10),
          finish: null,
          popularity: 12,
          raceDate: "20260510",
        }),
      ]}
    />,
  );
  const badges = Array.from(
    container.querySelectorAll<HTMLElement>(".race-results-correlation-rank-badge"),
  );
  expect(badges.map((badge) => badge.textContent)).toStrictEqual(["1", "-", "6", "12"]);
  expect(badges.map((badge) => badge.style.backgroundColor)).toStrictEqual([
    "#d4a017",
    "#e5e7eb",
    "#8aa0ae",
    "#c9d2d8",
  ]);
  expect(badges.map((badge) => badge.style.color)).toStrictEqual([
    "#ffffff",
    "#1f2937",
    "#ffffff",
    "#1f2937",
  ]);
});

test("renders the market-gap badges with per-tone classes and signed labels", () => {
  const { container } = render(
    <HorseRaceResultsCorrelationMatrix
      rows={[
        correlationRow({ finish: 1, popularity: 6 }),
        correlationRow({
          dateValue: Date.UTC(2026, 4, 10),
          finish: 8,
          popularity: 2,
          raceDate: "20260510",
        }),
        correlationRow({
          dateValue: Date.UTC(2026, 5, 1),
          finish: 3,
          popularity: 3,
          raceDate: "20260601",
        }),
      ]}
    />,
  );
  const badges = Array.from(
    container.querySelectorAll<HTMLElement>(".race-results-correlation-gap-badge"),
  );
  expect(badges.map((badge) => badge.textContent)).toStrictEqual(["-5", "+6", "±0"]);
  expect(badges.map((badge) => badge.getAttribute("data-tone"))).toStrictEqual([
    "over",
    "under",
    "even",
  ]);
  expect(badges.map((badge) => badge.className)).toStrictEqual([
    "race-results-correlation-gap-badge race-results-correlation-gap-over",
    "race-results-correlation-gap-badge race-results-correlation-gap-under",
    "race-results-correlation-gap-badge race-results-correlation-gap-even",
  ]);
});

test("renders the weight sparkline with per-column value labels", () => {
  const { container } = render(
    <HorseRaceResultsCorrelationMatrix
      rows={[
        correlationRow({ weight: 480 }),
        correlationRow({ dateValue: Date.UTC(2026, 3, 5), raceDate: "20260405", weight: null }),
        correlationRow({ dateValue: Date.UTC(2026, 4, 10), raceDate: "20260510", weight: 490 }),
      ]}
    />,
  );
  const svg = container.querySelector("svg");
  expect(svg?.getAttribute("viewBox")).toStrictEqual("0 0 132 48");
  expect(
    Array.from(container.querySelectorAll("polyline")).map((line) => line.getAttribute("points")),
  ).toStrictEqual(["22,42", "110,6"]);
  expect(
    Array.from(container.querySelectorAll("polyline")).map((line) => line.getAttribute("stroke")),
  ).toStrictEqual(["#0f766e", "#0f766e"]);
  expect(container.querySelectorAll("circle").length).toStrictEqual(2);
  expect(
    Array.from(container.querySelectorAll(".race-results-correlation-weight-label")).map(
      (label) => label.textContent,
    ),
  ).toStrictEqual(["480", "-", "490"]);
});

test("renders the futan row with one-decimal and integer kilogram values", () => {
  const { container } = render(
    <HorseRaceResultsCorrelationMatrix
      rows={[
        correlationRow({ futan: 56 }),
        correlationRow({ dateValue: Date.UTC(2026, 3, 5), futan: 55.5, raceDate: "20260405" }),
        correlationRow({ dateValue: Date.UTC(2026, 4, 10), futan: null, raceDate: "20260510" }),
      ]}
    />,
  );
  expect(
    Array.from(container.querySelectorAll(".race-results-correlation-futan-label")).map(
      (cell) => cell.textContent,
    ),
  ).toStrictEqual(["56", "55.5", "-"]);
});

test("renders signed delta bars with per-sign colors and labels", () => {
  const { container } = render(
    <HorseRaceResultsCorrelationMatrix
      rows={[
        correlationRow({ weightDelta: 6 }),
        correlationRow({
          dateValue: Date.UTC(2026, 4, 10),
          raceDate: "20260510",
          weightDelta: -4,
        }),
      ]}
    />,
  );
  const upperBars = Array.from(
    container.querySelectorAll<HTMLElement>(
      ".race-results-correlation-delta-half.upper .race-results-correlation-delta-bar",
    ),
  );
  const lowerBars = Array.from(
    container.querySelectorAll<HTMLElement>(
      ".race-results-correlation-delta-half.lower .race-results-correlation-delta-bar",
    ),
  );
  expect(upperBars.map((bar) => bar.style.backgroundColor)).toStrictEqual(["#ea580c"]);
  expect(upperBars.map((bar) => bar.style.height)).toStrictEqual(["100%"]);
  expect(lowerBars.map((bar) => bar.style.backgroundColor)).toStrictEqual(["#2563eb"]);
  expect(lowerBars.map((bar) => bar.style.height)).toStrictEqual(["67%"]);
  expect(
    Array.from(container.querySelectorAll(".race-results-correlation-delta-label")).map(
      (label) => label.textContent,
    ),
  ).toStrictEqual(["+6", "-4"]);
});

test("highlights the upcoming-race column across every metric row", () => {
  const { container } = render(
    <HorseRaceResultsCorrelationMatrix
      rows={[
        correlationRow({ finish: 1, futan: 56, popularity: 2, weight: 480 }),
        correlationRow({
          dateValue: Date.UTC(2026, 5, 1),
          finish: null,
          futan: null,
          popularity: null,
          raceDate: "20260601",
          weight: 486,
          weightDelta: 6,
        }),
      ]}
    />,
  );
  expect(
    Array.from(
      container.querySelectorAll(
        ".race-results-correlation-date.race-results-correlation-upcoming",
      ),
    ).map((cell) => cell.textContent),
  ).toStrictEqual(["26/06/01"]);
  expect(
    Array.from(
      container.querySelectorAll(
        ".race-results-correlation-weight-label.race-results-correlation-upcoming",
      ),
    ).map((cell) => cell.textContent),
  ).toStrictEqual(["486"]);
  expect(
    Array.from(
      container.querySelectorAll(
        ".race-results-correlation-futan-label.race-results-correlation-upcoming",
      ),
    ).map((cell) => cell.textContent),
  ).toStrictEqual(["-"]);
  expect(container.querySelectorAll(".race-results-correlation-upcoming").length).toStrictEqual(7);
});

test("renders no upcoming highlight when every race has a finish", () => {
  const { container } = render(
    <HorseRaceResultsCorrelationMatrix
      rows={[
        correlationRow({ finish: 1, popularity: 2, weight: 480 }),
        correlationRow({
          dateValue: Date.UTC(2026, 4, 10),
          finish: 3,
          popularity: 4,
          raceDate: "20260510",
          weight: 482,
        }),
      ]}
    />,
  );
  expect(container.querySelectorAll(".race-results-correlation-upcoming").length).toStrictEqual(0);
});

test("keeps the upcoming-race finish and popularity badges empty", () => {
  const { container } = render(
    <HorseRaceResultsCorrelationMatrix
      rows={[
        correlationRow({ finish: 1, popularity: 2, weight: 480 }),
        correlationRow({
          dateValue: Date.UTC(2026, 5, 1),
          finish: null,
          futan: null,
          popularity: null,
          raceDate: "20260601",
          weight: 486,
          weightDelta: 6,
        }),
      ]}
    />,
  );
  expect(
    Array.from(container.querySelectorAll(".race-results-correlation-rank-badge")).map(
      (badge) => badge.textContent,
    ),
  ).toStrictEqual(["1", "-", "2", "-"]);
});

test("renders a gray stub for a zero delta and only a dash for a missing delta", () => {
  const { container } = render(
    <HorseRaceResultsCorrelationMatrix
      rows={[
        correlationRow({ weightDelta: 0 }),
        correlationRow({
          dateValue: Date.UTC(2026, 4, 10),
          raceDate: "20260510",
          weightDelta: null,
        }),
      ]}
    />,
  );
  const upperBars = Array.from(
    container.querySelectorAll<HTMLElement>(
      ".race-results-correlation-delta-half.upper .race-results-correlation-delta-bar",
    ),
  );
  expect(upperBars.map((bar) => bar.style.backgroundColor)).toStrictEqual(["#9ca3af"]);
  expect(upperBars.map((bar) => bar.style.height)).toStrictEqual(["8%"]);
  expect(
    container.querySelectorAll(
      ".race-results-correlation-delta-half.lower .race-results-correlation-delta-bar",
    ).length,
  ).toStrictEqual(0);
  expect(
    Array.from(container.querySelectorAll(".race-results-correlation-delta-label")).map(
      (label) => label.textContent,
    ),
  ).toStrictEqual(["±0", "-"]);
});
