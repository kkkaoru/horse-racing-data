// bun で実行する (bunx vitest)
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";

import type { RaceTimeStats } from "../../../lib/race-types";
import {
  formatRaceTimeDecimalTenths,
  formatRaceTimeTenths,
  RaceTimeStatsMetrics,
} from "./race-time-stats-metrics";

afterEach(() => {
  cleanup();
});

const stats: RaceTimeStats = {
  averageKohan3f: 345,
  averageRaceTime: 965,
  correlationRows: [],
  fastestDetail: null,
  fastestKohan3f: 332,
  fastestRaceTime: 725,
  medianKohan3f: 341,
  medianRaceTime: 960,
  raceCount: 12,
  targetRaces: [],
};

it("formats tenths race times with and without minutes", () => {
  expect(formatRaceTimeTenths(null)).toBe("-");
  expect(formatRaceTimeTenths(725)).toBe("1:12.5");
  expect(formatRaceTimeTenths(345)).toBe("34.5");
});

it("formats tenths as a one-decimal value", () => {
  expect(formatRaceTimeDecimalTenths(null)).toBe("-");
  expect(formatRaceTimeDecimalTenths(332)).toBe("33.2");
});

it("renders compact time-trend metrics", () => {
  render(<RaceTimeStatsMetrics stats={stats} />);
  expect(screen.getByLabelText("タイム傾向")).toBeDefined();
  expect(screen.getByText("最速レースタイム")).toBeDefined();
  expect(screen.getByText("1:12.5")).toBeDefined();
  expect(screen.getByText("最速上がり3F")).toBeDefined();
  expect(screen.getByText("33.2")).toBeDefined();
  expect(screen.getByText("平均レースタイム")).toBeDefined();
  expect(screen.getByText("1:36.5")).toBeDefined();
  expect(screen.getByText("平均上がり3F")).toBeDefined();
  expect(screen.getByText("34.5")).toBeDefined();
  expect(screen.getByText("中央値レースタイム")).toBeDefined();
  expect(screen.getByText("1:36.0")).toBeDefined();
  expect(screen.getByText("中央値上がり3F")).toBeDefined();
  expect(screen.getByText("34.1")).toBeDefined();
});
