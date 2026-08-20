// bun で実行する (bunx vitest)
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import type { RaceTimeStats } from "../../../lib/race-types";
import { RaceConditionAnalysisSection } from "./race-condition-analysis-section";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

vi.mock("next/navigation", () => ({
  useRouter: () => ({
    replace: vi.fn<() => void>(),
  }),
}));

const raceTimeStats: RaceTimeStats = {
  averageKohan3f: null,
  averageRaceTime: null,
  correlationRows: [],
  fastestDetail: null,
  fastestKohan3f: null,
  fastestRaceTime: null,
  medianKohan3f: null,
  medianRaceTime: null,
  raceCount: 0,
  targetRaces: [],
};

it("does not render condition filters, time-trend records, or the former frame-order table", () => {
  vi.stubGlobal(
    "matchMedia",
    vi.fn(() => ({
      addEventListener: vi.fn<() => void>(),
      addListener: vi.fn<() => void>(),
      matches: false,
      removeEventListener: vi.fn<() => void>(),
      removeListener: vi.fn<() => void>(),
    })),
  );
  render(
    <RaceConditionAnalysisSection
      finishPositionStats={[]}
      payoutStats={[]}
      raceTimeStats={raceTimeStats}
    />,
  );
  expect(screen.queryByRole("heading", { name: "枠順分析" })).toBeNull();
  expect(screen.queryByText("枠順分析")).toBeNull();
  expect(screen.queryByText("条件設定")).toBeNull();
  expect(screen.queryByText("全ての条件を外す")).toBeNull();
  expect(screen.queryByRole("heading", { name: "タイム傾向" })).toBeNull();
  expect(screen.queryByText("最速レースタイム")).toBeNull();
  expect(screen.queryByText("該当する最速レースはありません。")).toBeNull();
  expect(screen.getByRole("heading", { name: "対象レース一覧" })).toBeDefined();
  expect(screen.getByRole("heading", { name: "払い戻し傾向" })).toBeDefined();
  expect(screen.getByRole("heading", { name: "着順別 人気・オッズ" })).toBeDefined();
});
