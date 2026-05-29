import type { RealtimeOddsTrend } from "horse-racing-realtime/types";
import { expect, test } from "vitest";

import {
  getDisplayTrends,
  sortOddsTrendEntries,
  type OddsTrendHoverEntry,
} from "./realtime-race-section";

const buildTrend = (combination: string, latestOdds: number | null): RealtimeOddsTrend => ({
  combination,
  points:
    latestOdds === null
      ? []
      : [
          {
            combination,
            fetchedAt: "2026-05-25T12:00:00+09:00",
            odds: latestOdds,
            rank: null,
          },
        ],
});

const buildEntry = (overrides: Partial<OddsTrendHoverEntry>): OddsTrendHoverEntry => ({
  color: "#000",
  dataKey: "odds_1",
  name: "1",
  value: 0,
  ...overrides,
});

test("sortOddsTrendEntries orders entries by odds value descending (high odds first)", () => {
  const sorted = sortOddsTrendEntries([
    buildEntry({ dataKey: "odds_1", value: 1.5 }),
    buildEntry({ dataKey: "odds_2", value: 12.4 }),
    buildEntry({ dataKey: "odds_3", value: 4.2 }),
  ]);
  expect(sorted.map((entry) => entry.value)).toStrictEqual([12.4, 4.2, 1.5]);
});

test("sortOddsTrendEntries falls back to horse-number ascending on tied odds", () => {
  const sorted = sortOddsTrendEntries([
    buildEntry({ dataKey: "odds_5", value: 3.0 }),
    buildEntry({ dataKey: "odds_2", value: 3.0 }),
    buildEntry({ dataKey: "odds_7", value: 3.0 }),
  ]);
  expect(sorted.map((entry) => entry.dataKey)).toStrictEqual(["odds_2", "odds_5", "odds_7"]);
});

test("sortOddsTrendEntries drops entries whose value is not numeric", () => {
  const sorted = sortOddsTrendEntries([
    buildEntry({ dataKey: "odds_1", value: 1.5 }),
    buildEntry({ dataKey: "odds_2", value: null }),
    buildEntry({ dataKey: "odds_3", value: "10" }),
  ]);
  expect(sorted).toHaveLength(1);
  expect(sorted[0]?.value).toBe(1.5);
});

test("sortOddsTrendEntries returns an empty array when given no entries", () => {
  expect(sortOddsTrendEntries([])).toStrictEqual([]);
});

test("getDisplayTrends sorts tansho trends DESC by latest odds", () => {
  const sorted = getDisplayTrends("tansho", [
    buildTrend("1", 1.5),
    buildTrend("2", 12.4),
    buildTrend("3", 4.2),
  ]);
  expect(sorted.map((trend) => trend.combination)).toStrictEqual(["2", "3", "1"]);
});

test("getDisplayTrends drops trends that have no numeric odds points", () => {
  const sorted = getDisplayTrends("tansho", [buildTrend("1", null), buildTrend("2", 3.1)]);
  expect(sorted.map((trend) => trend.combination)).toStrictEqual(["2"]);
});

test("getDisplayTrends for non-tansho keeps the top-N most-bet trims then DESC-sorts the survivors", () => {
  const trends: RealtimeOddsTrend[] = Array.from({ length: 25 }, (_, index) =>
    buildTrend(`${index + 1}`, index + 1),
  );
  const sorted = getDisplayTrends("umaren", trends);
  // top 20 most-bet (lowest odds) = combinations "1".."20", then displayed DESC by odds → "20" first
  expect(sorted[0]?.combination).toBe("20");
  expect(sorted[sorted.length - 1]?.combination).toBe("1");
  expect(sorted).toHaveLength(20);
});
