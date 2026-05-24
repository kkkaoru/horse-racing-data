import { expect, test } from "vitest";

import {
  collectYearsFromRows,
  extractYearFromRaceId,
  parsePredictionLine,
  summarize,
  withBaseline,
} from "./walk-forward-win5-eval";

const SAMPLE_LINE = '{"race_id":"jra:2024:0107:06:01","ketto_toroku_bango":"2020001234","predicted_score":0.875,"predicted_rank":1,"umaban":1}';

test("extractYearFromRaceId returns the year part", () => {
  expect(extractYearFromRaceId("jra:2024:0107:06:01")).toBe(2024);
});

test("extractYearFromRaceId returns null on malformed id", () => {
  expect(extractYearFromRaceId("invalid")).toBe(null);
});

test("parsePredictionLine parses a valid JSONL row", () => {
  expect(parsePredictionLine(SAMPLE_LINE)).toStrictEqual({
    race_id: "jra:2024:0107:06:01",
    ketto_toroku_bango: "2020001234",
    predicted_score: 0.875,
  });
});

test("parsePredictionLine returns null on empty input", () => {
  expect(parsePredictionLine("")).toBe(null);
});

test("parsePredictionLine returns null when fields are missing", () => {
  expect(parsePredictionLine('{"race_id":"x","ketto_toroku_bango":"y"}')).toBe(null);
});

test("collectYearsFromRows returns sorted unique years", () => {
  expect(
    collectYearsFromRows([
      {
        kaisai_nen: "2024",
        kaisai_tsukihi: "0101",
        race_joho_1: null,
        race_joho_2: null,
        race_joho_3: null,
        race_joho_4: null,
        race_joho_5: null,
        haraimodoshi_win5_001: null,
        tekichu_nashi_flag: null,
        fuseiritsu_flag: null,
      },
      {
        kaisai_nen: "2022",
        kaisai_tsukihi: "0202",
        race_joho_1: null,
        race_joho_2: null,
        race_joho_3: null,
        race_joho_4: null,
        race_joho_5: null,
        haraimodoshi_win5_001: null,
        tekichu_nashi_flag: null,
        fuseiritsu_flag: null,
      },
      {
        kaisai_nen: "2024",
        kaisai_tsukihi: "0303",
        race_joho_1: null,
        race_joho_2: null,
        race_joho_3: null,
        race_joho_4: null,
        race_joho_5: null,
        haraimodoshi_win5_001: null,
        tekichu_nashi_flag: null,
        fuseiritsu_flag: null,
      },
    ]),
  ).toStrictEqual([2022, 2024]);
});

test("summarize computes default and recommended rates", () => {
  const result = summarize({
    startYear: 2007,
    endYear: 2025,
    skippedDays: 3,
    results: [
      {
        actualWinners: ["1", "2", "3", "4", "5"],
        defaultBudgetYen: 2000,
        defaultCostYen: 1600,
        defaultHit: true,
        defaultReturnYen: 100_000,
        kaisaiNen: "2024",
        kaisaiTsukihi: "0101",
        payoutYen: 100_000,
        recommendedBudgetYen: 5000,
        recommendedCostYen: 4800,
        recommendedHit: true,
        recommendedReturnYen: 100_000,
      },
      {
        actualWinners: ["1", "2", "3", "4", "5"],
        defaultBudgetYen: 2000,
        defaultCostYen: 1600,
        defaultHit: false,
        defaultReturnYen: 0,
        kaisaiNen: "2024",
        kaisaiTsukihi: "0102",
        payoutYen: 200_000,
        recommendedBudgetYen: 5000,
        recommendedCostYen: 4800,
        recommendedHit: false,
        recommendedReturnYen: 0,
      },
    ],
  });
  expect(result.defaultHitCount).toBe(1);
  expect(result.defaultHitRate).toBe(50);
  expect(result.defaultRecoveryRate).toBe(3125);
  expect(result.recommendedHitRate).toBe(50);
  expect(result.skippedDays).toBe(3);
  expect(result.evaluatedDays).toBe(2);
  expect(result.recommendedBudgetAverageYen).toBe(5000);
});

test("summarize returns zero rates when no results", () => {
  const result = summarize({
    startYear: 2007,
    endYear: 2025,
    skippedDays: 1,
    results: [],
  });
  expect(result.defaultHitRate).toBe(0);
  expect(result.defaultRecoveryRate).toBe(0);
});

test("withBaseline returns plain summary when baseline is null", () => {
  const summary = summarize({ startYear: 2007, endYear: 2025, skippedDays: 0, results: [] });
  expect(withBaseline(summary, null)).toStrictEqual(summary);
});

test("withBaseline computes deltas when baseline is provided", () => {
  const summary = summarize({ startYear: 2007, endYear: 2025, skippedDays: 0, results: [] });
  const result = withBaseline(summary, {
    defaultHitRate: 1.95,
    defaultRecoveryRate: 30.99,
    recommendedHitRate: 1.95,
    recommendedRecoveryRate: 30.99,
  });
  expect(result.delta?.defaultHitRate).toBe(-1.95);
  expect(result.delta?.defaultRecoveryRate).toBe(-30.99);
});
