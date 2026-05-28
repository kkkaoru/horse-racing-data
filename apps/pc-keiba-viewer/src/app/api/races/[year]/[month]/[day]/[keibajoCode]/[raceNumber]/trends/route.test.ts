// Run with bun.
import { expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

vi.mock("@opennextjs/cloudflare", () => ({
  getCloudflareContext: vi.fn<() => Promise<{ ctx: null; env: null }>>(async () => ({
    ctx: null,
    env: null,
  })),
}));

import { isCacheableTrendPayload } from "./route";
import type {
  RaceTrendRawPayload,
  RaceTrendStarterRow,
} from "../../../../../../../../../lib/race-types";

const buildStarterRow = (overrides: Partial<RaceTrendStarterRow> = {}): RaceTrendStarterRow => ({
  bamei: "テスト",
  bataiju: null,
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  finishPosition: 1,
  hassoJikoku: null,
  jockeyName: "騎手",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0524",
  keibajoCode: "47",
  raceBango: "01",
  raceName: null,
  runnerCount: null,
  sohaTime: null,
  source: "nar",
  tanshoOdds: null,
  tanshoPopularity: null,
  umaban: "01",
  wakuban: "1",
  zogenFugo: null,
  zogenSa: null,
  ...overrides,
});

const buildPayload = (overrides: Partial<RaceTrendRawPayload> = {}): RaceTrendRawPayload => ({
  currentRunningStyles: [],
  historicalRunningStyles: [],
  raceContext: { keibajoCode: "42", raceBango: "01", source: "nar" },
  runners: [],
  starterRows: [],
  ...overrides,
});

it("isCacheableTrendPayload rejects a payload with neither starter rows nor running-style history", () => {
  expect(isCacheableTrendPayload(buildPayload())).toBe(false);
});

it("isCacheableTrendPayload rejects a payload with starter rows but empty running-style history", () => {
  expect(
    isCacheableTrendPayload(
      buildPayload({
        starterRows: [buildStarterRow()],
        historicalRunningStyles: [],
      }),
    ),
  ).toBe(false);
});

it("isCacheableTrendPayload rejects a payload with running-style history but no starter rows", () => {
  expect(
    isCacheableTrendPayload(
      buildPayload({
        starterRows: [],
        historicalRunningStyles: [
          { horseNumber: "1", predictedLabel: "nige", raceKey: "nar:20260524:47:01" },
        ],
      }),
    ),
  ).toBe(false);
});

it("isCacheableTrendPayload accepts a payload with both starter rows and running-style history", () => {
  expect(
    isCacheableTrendPayload(
      buildPayload({
        starterRows: [buildStarterRow()],
        historicalRunningStyles: [
          { horseNumber: "1", predictedLabel: "nige", raceKey: "nar:20260524:47:01" },
        ],
      }),
    ),
  ).toBe(true);
});

it("isCacheableTrendPayload accepts a populated 14-day-window payload", () => {
  const starterRows = Array.from({ length: 3849 }, (_, index) =>
    buildStarterRow({ umaban: String(index + 1) }),
  );
  const historicalRunningStyles = Array.from({ length: 2416 }, (_, index) => ({
    horseNumber: String((index % 12) + 1),
    predictedLabel: "sashi" as const,
    raceKey: `nar:20260524:47:${String((index % 12) + 1).padStart(2, "0")}`,
  }));
  expect(isCacheableTrendPayload(buildPayload({ starterRows, historicalRunningStyles }))).toBe(
    true,
  );
});
