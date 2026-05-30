// Run with bun.
import { expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

vi.mock("@opennextjs/cloudflare", () => ({
  getCloudflareContext: vi.fn<() => Promise<{ ctx: null; env: null }>>(async () => ({
    ctx: null,
    env: null,
  })),
}));

import type {
  RaceTrendDailyTrackRow,
  RaceTrendStarterRow,
} from "horse-racing-realtime/race-trend-daily-track-types";

import type { RaceTrendDailyTrackFetchResult } from "../../../../../../../../../lib/race-trend-daily-track-client.server";
import type { RaceTrendRawPayload } from "../../../../../../../../../lib/race-types";
import {
  filterTodaySiblingRows,
  isCacheableTrendPayload,
  pickTodaySiblingRowsAndSource,
} from "./route";

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

it("filterTodaySiblingRows keeps rows with the same source, date, venue and a smaller raceBango", () => {
  const sibling = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "50",
    raceBango: "03",
    source: "nar",
  });
  const target = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "50",
    raceBango: "07",
    source: "nar",
  });
  const after = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "50",
    raceBango: "08",
    source: "nar",
  });
  const result = filterTodaySiblingRows([sibling, target, after], {
    keibajoCode: "50",
    raceBango: "07",
    source: "nar",
    targetYmd: "20260529",
  });
  expect(result).toStrictEqual([sibling]);
});

it("filterTodaySiblingRows drops rows from a different venue", () => {
  const otherVenue = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "47",
    raceBango: "01",
    source: "nar",
  });
  expect(
    filterTodaySiblingRows([otherVenue], {
      keibajoCode: "50",
      raceBango: "07",
      source: "nar",
      targetYmd: "20260529",
    }),
  ).toStrictEqual([]);
});

it("filterTodaySiblingRows drops rows from a different source", () => {
  const otherSource = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "50",
    raceBango: "01",
    source: "jra",
  });
  expect(
    filterTodaySiblingRows([otherSource], {
      keibajoCode: "50",
      raceBango: "07",
      source: "nar",
      targetYmd: "20260529",
    }),
  ).toStrictEqual([]);
});

it("filterTodaySiblingRows drops rows whose date does not match the target ymd", () => {
  const otherDay = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0528",
    keibajoCode: "50",
    raceBango: "01",
    source: "nar",
  });
  expect(
    filterTodaySiblingRows([otherDay], {
      keibajoCode: "50",
      raceBango: "07",
      source: "nar",
      targetYmd: "20260529",
    }),
  ).toStrictEqual([]);
});

it("filterTodaySiblingRows excludes the target race itself", () => {
  const target = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "50",
    raceBango: "07",
    source: "nar",
  });
  expect(
    filterTodaySiblingRows([target], {
      keibajoCode: "50",
      raceBango: "07",
      source: "nar",
      targetYmd: "20260529",
    }),
  ).toStrictEqual([]);
});

it("filterTodaySiblingRows falls back to locale compare when raceBango is non-numeric", () => {
  const siblingA = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "50",
    raceBango: "A",
    source: "nar",
  });
  expect(
    filterTodaySiblingRows([siblingA], {
      keibajoCode: "50",
      raceBango: "B",
      source: "nar",
      targetYmd: "20260529",
    }),
  ).toStrictEqual([siblingA]);
});

const buildDailyTrackRow = (
  raceBango: string,
  starterRows: RaceTrendStarterRow[],
): RaceTrendDailyTrackRow => ({
  fetchedAt: "2026-05-29T07:30:00.000Z",
  finishedAt: "2026-05-29T07:20:00.000Z",
  isComplete: true,
  raceBango,
  raceKey: `jra:2026:0529:05:${raceBango}`,
  runningStyles: [],
  starterRows,
});

it("pickTodaySiblingRowsAndSource returns DO rows and do-hit header when DO result status is hit", () => {
  const doRow = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "05",
    raceBango: "01",
    source: "jra",
    umaban: "03",
  });
  const fallbackRow = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "05",
    raceBango: "02",
    source: "jra",
    umaban: "07",
  });
  const result: RaceTrendDailyTrackFetchResult = {
    rows: [buildDailyTrackRow("01", [doRow])],
    status: "hit",
  };
  expect(pickTodaySiblingRowsAndSource({ fallbackRows: [fallbackRow], result })).toStrictEqual({
    rows: [doRow],
    sourceHeader: "do-hit",
  });
});

it("pickTodaySiblingRowsAndSource flattens starterRows across multiple DO race rows when status is hit", () => {
  const doRowA = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "05",
    raceBango: "01",
    source: "jra",
    umaban: "01",
  });
  const doRowB = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "05",
    raceBango: "02",
    source: "jra",
    umaban: "02",
  });
  const result: RaceTrendDailyTrackFetchResult = {
    rows: [buildDailyTrackRow("01", [doRowA]), buildDailyTrackRow("02", [doRowB])],
    status: "hit",
  };
  expect(pickTodaySiblingRowsAndSource({ fallbackRows: [], result })).toStrictEqual({
    rows: [doRowA, doRowB],
    sourceHeader: "do-hit",
  });
});

it("pickTodaySiblingRowsAndSource falls back to legacy rows with do-miss-fallback header when DO result status is miss", () => {
  const fallbackRow = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "47",
    raceBango: "04",
    source: "nar",
    umaban: "05",
  });
  const result: RaceTrendDailyTrackFetchResult = { rows: [], status: "miss" };
  expect(pickTodaySiblingRowsAndSource({ fallbackRows: [fallbackRow], result })).toStrictEqual({
    rows: [fallbackRow],
    sourceHeader: "do-miss-fallback",
  });
});

it("pickTodaySiblingRowsAndSource falls back to legacy rows with do-error-fallback header when DO result status is error", () => {
  const fallbackRow = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "47",
    raceBango: "06",
    source: "nar",
    umaban: "08",
  });
  const result: RaceTrendDailyTrackFetchResult = { rows: [], status: "error" };
  expect(pickTodaySiblingRowsAndSource({ fallbackRows: [fallbackRow], result })).toStrictEqual({
    rows: [fallbackRow],
    sourceHeader: "do-error-fallback",
  });
});

it("pickTodaySiblingRowsAndSource returns an empty rows array when both DO is miss and fallback is empty", () => {
  const result: RaceTrendDailyTrackFetchResult = { rows: [], status: "miss" };
  expect(pickTodaySiblingRowsAndSource({ fallbackRows: [], result })).toStrictEqual({
    rows: [],
    sourceHeader: "do-miss-fallback",
  });
});
