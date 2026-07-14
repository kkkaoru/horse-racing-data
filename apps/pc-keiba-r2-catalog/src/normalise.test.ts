import { expect, it } from "vitest";

import { normaliseCatalogRaceKeyRow, normaliseDailyRaceEntryRow } from "./normalise";

const baseRaw = (): Record<string, unknown> => ({
  kaisai_nen: "2026",
  kaisai_tsukihi: "0715",
  keibajo_code: "5",
  ketto_toroku_bango: "2023100001",
  race_bango: "1",
  race_date: "20260715",
  source: "jra",
});

it("normalises the complete feature row into the Hyperdrive result shape", () => {
  const row = normaliseDailyRaceEntryRow({
    ...baseRaw(),
    babajotai_code_dirt: true,
    babajotai_code_shiba: 2,
    bamei: { name: "Horse" },
    banushimei: 9n,
    barei: "3",
    bataiju: 480,
    chokyoshimei_ryakusho: "Trainer",
    corner1_norm: "0.1",
    corner2_norm: 0.2,
    corner3_norm: Number.NaN,
    corner4_norm: "bad",
    corner_1: "1",
    corner_2: 2,
    corner_3: "",
    corner_4: {},
    finish_norm: "0.25",
    finish_position: "2",
    futan_juryo: "55.5",
    grade_code: "A",
    hasso_jikoku: "1530",
    juryo_shubetsu_code: "1",
    kishumei_ryakusho: "Jockey",
    kohan_3f: "34.2",
    kyori: "1600",
    kyoso_joken_code: "005",
    kyoso_shubetsu_code: "11",
    race_name: "Main race",
    seibetsu_code: "1",
    shusso_tosu: 16,
    soha_time: "945",
    tansho_ninkijun: "4",
    tansho_odds: "12.3",
    time_sa: "0.4",
    track_code: "17",
    umaban: "7",
    wakuban: "4",
    zogen_fugo: "+",
    zogen_sa: "6",
  });

  expect(row).toStrictEqual({
    babajotai_code_dirt: "true",
    babajotai_code_shiba: "2",
    bamei: '{"name":"Horse"}',
    banushimei: "9",
    barei: 3,
    bataiju: 480,
    chokyoshimei_ryakusho: "Trainer",
    corner1_norm: 0.1,
    corner2_norm: 0.2,
    corner3_norm: null,
    corner4_norm: null,
    corner_1: 1,
    corner_2: 2,
    corner_3: null,
    corner_4: null,
    finish_norm: 0.25,
    finish_position: 2,
    futan_juryo: 55.5,
    grade_code: "A",
    hasso_jikoku: "1530",
    juryo_shubetsu_code: "1",
    kaisai_nen: "2026",
    kaisai_tsukihi: "0715",
    keibajo_code: "05",
    ketto_toroku_bango: "2023100001",
    kishumei_ryakusho: "Jockey",
    kohan_3f: 34.2,
    kyori: 1600,
    kyoso_joken_code: "005",
    kyoso_shubetsu_code: "11",
    race_bango: "01",
    race_date: "20260715",
    race_name: "Main race",
    seibetsu_code: "1",
    shusso_tosu: 16,
    soha_time: 945,
    source: "jra",
    tansho_ninkijun: 4,
    tansho_odds: 12.3,
    time_sa: 0.4,
    track_code: "17",
    umaban: 7,
    wakuban: "4",
    zogen_fugo: "+",
    zogen_sa: 6,
  });
});

it("normalises missing optional feature values to null", () => {
  const row = normaliseDailyRaceEntryRow(baseRaw());
  expect(row.umaban).toBe(null);
  expect(row.bamei).toBe(null);
  expect(row.finish_position).toBe(null);
  expect(row.zogen_sa).toBe(null);
});

it("normalises an Iceberg race-key row into the raw Hyperdrive shape", () => {
  expect(
    normaliseCatalogRaceKeyRow({
      keibajo_code: 83,
      race_bango: 9,
      race_date: "2026-07-15T00:00:00Z",
      source: "nar",
    }),
  ).toStrictEqual({
    kaisai_nen: "2026",
    kaisai_tsukihi: "0715",
    keibajo_code: "83",
    race_bango: "09",
    source: "nar",
  });
});

it("rejects invalid required fields", () => {
  expect(() => normaliseDailyRaceEntryRow({ ...baseRaw(), source: "banei" })).toThrow(
    "R2 SQL row has invalid source: banei",
  );
  expect(() => normaliseDailyRaceEntryRow({ ...baseRaw(), kaisai_nen: null })).toThrow(
    "R2 SQL row is missing kaisai_nen",
  );
  expect(() => normaliseDailyRaceEntryRow({ ...baseRaw(), race_date: "2026/07/15" })).toThrow(
    "R2 SQL row has invalid race_date: 2026/07/15",
  );
});
