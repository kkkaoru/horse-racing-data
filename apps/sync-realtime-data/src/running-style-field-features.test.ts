// Run with bun test apps/sync-realtime-data/src/running-style-field-features.test.ts
import { expect, test } from "vitest";

import { computeFieldFeaturesPerHorse, type HorsePeerInputs } from "./running-style-field-features";

const HORSE_A: HorsePeerInputs = {
  careerWinRate: 0.2,
  kohan3fAvg5: 36.5,
  pastCorner1NormAvg5: 0.1,
  pastFirst3fAvg5: 35.8,
  pastNigeRate: 0.6,
  pastOikomiRate: 0.0,
  pastSashiRate: 0.0,
  pastSenkouRate: 0.4,
  speedIndexAvg5: 70,
  speedIndexBest5: 80,
};

const HORSE_B: HorsePeerInputs = {
  careerWinRate: 0.1,
  kohan3fAvg5: 36.2,
  pastCorner1NormAvg5: 0.4,
  pastFirst3fAvg5: 36.0,
  pastNigeRate: 0.1,
  pastOikomiRate: 0.1,
  pastSashiRate: 0.5,
  pastSenkouRate: 0.3,
  speedIndexAvg5: 65,
  speedIndexBest5: 75,
};

const HORSE_C: HorsePeerInputs = {
  careerWinRate: 0.05,
  kohan3fAvg5: 35.9,
  pastCorner1NormAvg5: 0.8,
  pastFirst3fAvg5: 36.4,
  pastNigeRate: 0.0,
  pastOikomiRate: 0.7,
  pastSashiRate: 0.2,
  pastSenkouRate: 0.1,
  speedIndexAvg5: 60,
  speedIndexBest5: 68,
};

test("field_nige_pressure excludes self horse", () => {
  const rows = computeFieldFeaturesPerHorse([HORSE_A, HORSE_B, HORSE_C]);
  const row = rows[0];
  expect(row?.field_nige_pressure).toBe(0.1);
});

test("field_senkou_pressure excludes self horse", () => {
  const rows = computeFieldFeaturesPerHorse([HORSE_A, HORSE_B, HORSE_C]);
  const row = rows[0];
  expect(row?.field_senkou_pressure).toBe(0.4);
});

test("field_pace_index uses weighted formula", () => {
  const rows = computeFieldFeaturesPerHorse([HORSE_A, HORSE_B, HORSE_C]);
  const row = rows[0];
  const diff = Math.abs((row?.field_pace_index ?? 0) - 0.6);
  expect(diff < 1e-9).toBe(true);
});

test("field_nige_candidate_count flags horses above 0.4 nige rate", () => {
  const rows = computeFieldFeaturesPerHorse([HORSE_A, HORSE_B, HORSE_C]);
  const row = rows[0];
  expect(row?.field_nige_candidate_count).toBe(0);
});

test("field_max_past_corner_1_norm reflects peer maximum", () => {
  const rows = computeFieldFeaturesPerHorse([HORSE_A, HORSE_B, HORSE_C]);
  const row = rows[0];
  expect(row?.field_max_past_corner_1_norm).toBe(0.8);
});

test("field_min_past_corner_1_norm reflects peer minimum", () => {
  const rows = computeFieldFeaturesPerHorse([HORSE_A, HORSE_B, HORSE_C]);
  const row = rows[0];
  expect(row?.field_min_past_corner_1_norm).toBe(0.4);
});

test("field_has_pure_nige_horse is true when peer nige rate exceeds threshold", () => {
  const horseHighNige: HorsePeerInputs = { ...HORSE_B, pastNigeRate: 0.85 };
  const rows = computeFieldFeaturesPerHorse([HORSE_A, horseHighNige, HORSE_C]);
  const row = rows[0];
  expect(row?.field_has_pure_nige_horse).toBe(true);
});

test("field_has_pure_nige_horse is false when no peer crosses threshold", () => {
  const rows = computeFieldFeaturesPerHorse([HORSE_A, HORSE_B, HORSE_C]);
  const row = rows[0];
  expect(row?.field_has_pure_nige_horse).toBe(false);
});

test("self_speed_index_vs_field_top equals 1 when self holds field max", () => {
  const rows = computeFieldFeaturesPerHorse([HORSE_A, HORSE_B, HORSE_C]);
  const row = rows[0];
  expect(row?.self_speed_index_vs_field_top).toBe(1);
});

test("self_nige_rate_minus_field_avg subtracts field nige average", () => {
  const rows = computeFieldFeaturesPerHorse([HORSE_A, HORSE_B, HORSE_C]);
  const row = rows[0];
  expect(row?.self_nige_rate_minus_field_avg).toBe(0.6 - 0.1 / 2);
});

test("null peer values are excluded from field sum", () => {
  const horseNoData: HorsePeerInputs = { ...HORSE_B, pastNigeRate: null };
  const rows = computeFieldFeaturesPerHorse([HORSE_A, horseNoData, HORSE_C]);
  const row = rows[0];
  expect(row?.field_nige_pressure).toBe(0);
});

test("all-null peer values yield null field pressure", () => {
  const horseNoData: HorsePeerInputs = { ...HORSE_B, pastNigeRate: null };
  const horseAlsoNoData: HorsePeerInputs = { ...HORSE_C, pastNigeRate: null };
  const rows = computeFieldFeaturesPerHorse([HORSE_A, horseNoData, horseAlsoNoData]);
  const row = rows[0];
  expect(row?.field_nige_pressure).toBe(null);
});

test("field_avg_speed_index averages peer speed indices", () => {
  const rows = computeFieldFeaturesPerHorse([HORSE_A, HORSE_B, HORSE_C]);
  const row = rows[0];
  expect(row?.field_avg_speed_index).toBe((65 + 60) / 2);
});

test("computeFieldFeaturesPerHorse returns one row per input horse", () => {
  const rows = computeFieldFeaturesPerHorse([HORSE_A, HORSE_B, HORSE_C]);
  expect(rows.length).toBe(3);
});

test("field_spread_past_corner_1_norm equals max minus min", () => {
  const rows = computeFieldFeaturesPerHorse([HORSE_A, HORSE_B, HORSE_C]);
  const row = rows[0];
  expect(row?.field_spread_past_corner_1_norm).toBe(0.8 - 0.4);
});
