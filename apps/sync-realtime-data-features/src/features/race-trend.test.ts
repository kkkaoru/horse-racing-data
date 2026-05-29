// Run with: bun run --filter sync-realtime-data-features test
import { expect, it } from "vitest";

import { aggregateRaceTrend, buildRaceTrendCacheKey } from "./race-trend";
import type { DailyRaceEntryRow } from "../types";

const baseRow: DailyRaceEntryRow = {
  babajotai_code_dirt: null,
  babajotai_code_shiba: null,
  bamei: null,
  banushimei: null,
  barei: null,
  bataiju: null,
  chokyoshimei_ryakusho: null,
  corner1_norm: null,
  corner2_norm: null,
  corner3_norm: null,
  corner4_norm: null,
  corner_1: null,
  corner_2: null,
  corner_3: null,
  corner_4: null,
  finish_norm: null,
  finish_position: null,
  futan_juryo: null,
  grade_code: null,
  hasso_jikoku: null,
  juryo_shubetsu_code: null,
  kaisai_nen: "2026",
  kaisai_tsukihi: "0529",
  keibajo_code: "30",
  ketto_toroku_bango: "kt",
  kishumei_ryakusho: null,
  kohan_3f: null,
  kyori: null,
  kyoso_joken_code: null,
  kyoso_shubetsu_code: null,
  race_bango: "08",
  race_date: "20260529",
  race_name: null,
  seibetsu_code: null,
  shusso_tosu: null,
  soha_time: null,
  source: "nar",
  tansho_ninkijun: null,
  tansho_odds: null,
  time_sa: null,
  track_code: null,
  umaban: null,
  wakuban: null,
  zogen_fugo: null,
  zogen_sa: null,
};

it("builds deterministic cache key", () => {
  expect(
    buildRaceTrendCacheKey({
      source: "nar",
      keibajoCode: "30",
      raceBango: "08",
      from: "20260501",
      to: "20260529",
    }),
  ).toBe("nar-30-08-20260501-20260529");
});

it("aggregates empty rows", () => {
  expect(aggregateRaceTrend([])).toStrictEqual({
    byJockey: {},
    byWaku: {},
    raceCount: 0,
    starterCount: 0,
  });
});

it("aggregates jockey and waku counts", () => {
  const rows: DailyRaceEntryRow[] = [
    { ...baseRow, kishumei_ryakusho: "山田", wakuban: "1" },
    { ...baseRow, kishumei_ryakusho: "山田", wakuban: "2" },
    { ...baseRow, kishumei_ryakusho: "佐藤", wakuban: "1" },
  ];
  const result = aggregateRaceTrend(rows);
  expect(result.raceCount).toBe(1);
  expect(result.starterCount).toBe(3);
  expect(result.byJockey["山田"]).toBe(2);
  expect(result.byWaku["1"]).toBe(2);
});

it("treats missing optional fields as not counted", () => {
  const result = aggregateRaceTrend([{ ...baseRow }]);
  expect(result.byJockey).toStrictEqual({});
  expect(result.byWaku).toStrictEqual({});
});

it("counts distinct race keys", () => {
  const rows: DailyRaceEntryRow[] = [{ ...baseRow }, { ...baseRow, race_bango: "09" }];
  expect(aggregateRaceTrend(rows).raceCount).toBe(2);
});
