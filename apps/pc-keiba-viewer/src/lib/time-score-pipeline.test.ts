// Run with bun.
import { expect, it } from "vitest";

import {
  buildTimeScoreRow,
  buildWeightedProfile,
  orderTimeScoreRows,
  postgresRound,
} from "./time-score-pipeline";
import type { TimeScoreCurrentHorse, TimeScoreHistoryRow } from "./time-score-pipeline";

const horse: TimeScoreCurrentHorse = {
  horseNumber: "08",
  horseNumberSort: 8,
  horseName: "テストホース",
  currentAge: 4,
};

const row = (overrides: Partial<TimeScoreHistoryRow> = {}): TimeScoreHistoryRow => ({
  horseNumber: "08",
  raceDate: "20260601",
  keibajoCode: "05",
  distance: 1600,
  raceTime: 940,
  last3f: 340,
  bodyWeight: 480,
  carriedWeight: 550,
  margin: 5,
  ...overrides,
});

it("rounds like PostgreSQL numeric (half away from zero)", () => {
  expect(postgresRound(1.005, 2)).toBe(1.01);
  expect(postgresRound(-1.005, 2)).toBe(-1.01);
  expect(postgresRound(2.675, 2)).toBe(2.68);
  expect(postgresRound(0.125, 2)).toBe(0.13);
  expect(postgresRound(1.449, 1)).toBe(1.4);
  expect(postgresRound(1234.5678, 2)).toBe(1234.57);
  expect(() => postgresRound(Number.NaN, 1)).toThrow("Invalid rounding input");
});

it("uses the FILTER denominators: null values never contribute to either side", () => {
  const profile = buildWeightedProfile(
    [row({ raceTime: null }), row({ raceTime: 900, keibajoCode: null })],
    horse,
    "20260920",
    1600,
    "05",
  );
  // Only the second row carries a race time, so the mean is that row alone.
  expect(profile.weightedRaceTime).toBe(900);
  // The venue denominator skips the null venue row and counts only the first.
  expect(profile.venueScore).toBe(1);
  expect(profile.distanceScore).toBe(1);
});

it("falls back to 0.5 when a denominator is zero", () => {
  const profile = buildWeightedProfile(
    [
      row({
        keibajoCode: null,
        distance: null,
        raceTime: null,
        last3f: null,
        bodyWeight: null,
        carriedWeight: null,
        margin: null,
      }),
    ],
    horse,
    "20260920",
    1600,
    "05",
  );
  expect(profile.venueScore).toBe(0.5);
  // A single row with a null distance still has a weight, so the mean is the
  // unknown-distance score rather than the zero-denominator fallback.
  expect(profile.distanceScore).toBe(0.5);
  expect(profile.weightedRaceTime).toBeNull();
  expect(profile.weightedLast3f).toBeNull();
});

it("returns the distance fallback when there are no rows at all", () => {
  const profile = buildWeightedProfile([], horse, "20260920", 1600, "05");
  expect(profile.distanceScore).toBe(0.5);
  expect(profile.venueScore).toBe(0.5);
});

it("emits seven details with the SQL labels, weights and reasons", () => {
  const profile = buildWeightedProfile([row()], horse, "20260920", 1600, "05");
  const result = buildTimeScoreRow(horse, profile, {
    targetRaceTime: 940,
    targetLast3f: 340,
    targetBodyWeight: 480,
    targetCarriedWeight: 550,
    targetMargin: 5,
  });
  expect(result.horseNumber).toBe("08");
  expect(result.horseName).toBe("テストホース");
  expect(result.details.map((detail) => detail.label)).toStrictEqual([
    "レースタイム",
    "上がり3F",
    "距離適性",
    "競馬場",
    "馬体重",
    "負担重量",
    "着差",
  ]);
  expect(result.details.map((detail) => detail.weight)).toStrictEqual([
    0.3, 0.2, 0.15, 0.15, 0.1, 0.05, 0.05,
  ]);
  expect(result.details.map((detail) => detail.reason)).toStrictEqual([
    "全ての過去成績を日付と今回距離への近さで重み付けし、同条件1〜3着馬の平均レースタイムに近いほど高評価",
    "全ての過去成績を日付と今回距離への近さで重み付けし、同条件1〜3着馬の平均上がり3Fに近いほど高評価",
    "全ての過去成績について、今回レース距離に近い成績ほど高く評価",
    "過去成績のうち今回と同じ競馬場の比率を日付の新しさで重み付け",
    "過去成績の馬体重を日付が新しいほど重く見て、同条件1〜3着馬の平均に近いほど高評価",
    "全ての過去成績の負担重量を日付と今回距離への近さで重み付けし、同条件1〜3着馬の平均に近いほど高評価",
    "全ての過去成績の着差を日付と今回距離への近さで重み付けし、同条件1〜3着馬の平均に近いほど高評価",
  ]);
  // Every value matches its target, so the weighted score is a perfect 1.
  expect(result.score).toBe(1);
  expect(result.details[0]?.value).toBe(940);
  expect(result.details[0]?.target).toBe(940);
  expect(result.details[2]?.value).toBe(100);
  expect(result.details[2]?.target).toBe(100);
  expect(result.details[0]?.score).toBe(1);
});

it("uses 0.5 for a sub-score whose value or target is null", () => {
  const profile = buildWeightedProfile([], horse, "20260920", 1600, "05");
  const result = buildTimeScoreRow(horse, profile, {
    targetRaceTime: null,
    targetLast3f: null,
    targetBodyWeight: null,
    targetCarriedWeight: null,
    targetMargin: null,
  });
  expect(result.details.map((detail) => detail.score)).toStrictEqual([
    0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5,
  ]);
  expect(result.details[0]?.value).toBeNull();
  expect(result.details[0]?.target).toBeNull();
  expect(result.score).toBe(0.5);
});

it("clamps a relative score at zero and uses the race-time denominator floor", () => {
  const profile = buildWeightedProfile([row({ raceTime: 2000 })], horse, "20260920", 1600, "05");
  const result = buildTimeScoreRow(horse, profile, {
    targetRaceTime: 1000,
    targetLast3f: 340,
    targetBodyWeight: 480,
    targetCarriedWeight: 550,
    targetMargin: 5,
  });
  // |2000 - 1000| / max(1000 * 0.08, 80) = 1000 / 80 -> clamped to 0.
  expect(result.details[0]?.score).toBe(0);
});

it("orders by score descending then horse-number sort ascending", () => {
  const rows = [
    { horseNumber: "01", horseName: "a", score: 0.5, details: [] },
    { horseNumber: "02", horseName: "b", score: 0.9, details: [] },
    { horseNumber: "03", horseName: "c", score: 0.5, details: [] },
  ];
  const sorts = new Map<string, number | null>([
    ["01", 1],
    ["02", 2],
    ["03", 3],
  ]);
  expect(orderTimeScoreRows(rows, sorts).map((item) => item.horseNumber)).toStrictEqual([
    "02",
    "01",
    "03",
  ]);
  expect(orderTimeScoreRows(rows, new Map()).map((item) => item.horseNumber)).toStrictEqual([
    "02",
    "01",
    "03",
  ]);
});
