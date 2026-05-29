// Run with: bun run --filter sync-realtime-data-features test
import { expect, it } from "vitest";

import { buildRaceKey, normaliseDailyRaceEntryRow, numericOrNull } from "./normalise";

it("returns null for null and undefined", () => {
  expect(numericOrNull(null)).toBeNull();
  expect(numericOrNull(undefined)).toBeNull();
});

it("returns the number when finite", () => {
  expect(numericOrNull(3.14)).toBe(3.14);
});

it("returns null for non-finite number", () => {
  expect(numericOrNull(Number.POSITIVE_INFINITY)).toBeNull();
});

it("parses numeric string", () => {
  expect(numericOrNull("42")).toBe(42);
});

it("returns null for non-numeric string", () => {
  expect(numericOrNull("abc")).toBeNull();
});

it("returns null for boolean", () => {
  expect(numericOrNull(true)).toBeNull();
});

it("normalises a raw NAR row preserving all 45 fields", () => {
  const raw = {
    babajotai_code_dirt: null,
    babajotai_code_shiba: null,
    bamei: "horse",
    banushimei: "owner",
    barei: 3,
    bataiju: 460,
    chokyoshimei_ryakusho: "trainer",
    corner1_norm: 0.1,
    corner2_norm: 0.2,
    corner3_norm: 0.3,
    corner4_norm: 0.4,
    corner_1: 5,
    corner_2: 6,
    corner_3: 7,
    corner_4: 8,
    finish_norm: 0.5,
    finish_position: 1,
    futan_juryo: 55.5,
    grade_code: null,
    hasso_jikoku: "1015",
    juryo_shubetsu_code: null,
    kaisai_nen: 2026,
    kaisai_tsukihi: 529,
    keibajo_code: "30",
    ketto_toroku_bango: "kt-1",
    kishumei_ryakusho: "jockey",
    kohan_3f: 36.5,
    kyori: 1600,
    kyoso_joken_code: null,
    kyoso_shubetsu_code: null,
    race_bango: "08",
    race_date: "20260529",
    race_name: "race",
    seibetsu_code: "1",
    shusso_tosu: 14,
    soha_time: 100,
    source: "nar",
    tansho_ninkijun: 3,
    tansho_odds: 4.2,
    time_sa: 0.5,
    track_code: "1",
    umaban: 1,
    wakuban: "1",
    zogen_fugo: "+",
    zogen_sa: 2,
  };
  const row = normaliseDailyRaceEntryRow(raw);
  expect(row.source).toBe("nar");
  expect(row.umaban).toBe(1);
  expect(row.kyori).toBe(1600);
});

it("converts numeric kaisai_nen to string", () => {
  const raw = {
    kaisai_nen: 2026,
    kaisai_tsukihi: 529,
    keibajo_code: 30,
    ketto_toroku_bango: 1,
    race_bango: 8,
    race_date: "20260529",
    source: "jra",
  };
  const row = normaliseDailyRaceEntryRow(raw);
  expect(row.kaisai_nen).toBe("2026");
});

it("throws for unexpected source", () => {
  expect(() => normaliseDailyRaceEntryRow({ source: "xxx" })).toThrowError(
    "unexpected source value: xxx",
  );
});

it("builds composite race key", () => {
  const row = normaliseDailyRaceEntryRow({
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
  });
  expect(buildRaceKey(row)).toBe("nar:20260529:30:08");
});

it("returns null for stringOrNull when raw is null", () => {
  const raw = {
    babajotai_code_dirt: null,
    bamei: null,
    kaisai_nen: "2026",
    kaisai_tsukihi: "0529",
    keibajo_code: "30",
    ketto_toroku_bango: "kt",
    race_bango: "08",
    race_date: "20260529",
    source: "nar",
  };
  const row = normaliseDailyRaceEntryRow(raw);
  expect(row.bamei).toBeNull();
});

it("converts non-string non-null value to string", () => {
  const raw = {
    bamei: 123,
    kaisai_nen: "2026",
    kaisai_tsukihi: "0529",
    keibajo_code: "30",
    ketto_toroku_bango: "kt",
    race_bango: "08",
    race_date: "20260529",
    source: "nar",
  };
  const row = normaliseDailyRaceEntryRow(raw);
  expect(row.bamei).toBe("123");
});
