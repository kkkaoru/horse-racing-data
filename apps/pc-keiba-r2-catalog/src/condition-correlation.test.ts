// Run with bun (bunx vitest).
import { expect, it } from "vitest";

import {
  buildConditionCorrelationCareerQuery,
  buildConditionCorrelationEntriesQuery,
  buildConditionCorrelationTargetQuery,
  composeConditionCorrelationRows,
} from "./condition-correlation";
import type { R2SqlCatalogConfig, WinRateHeatmapStatsFilters } from "./types";

const env: R2SqlCatalogConfig = {
  R2_SQL_ACCOUNT_ID: "account",
  R2_SQL_BUCKET_NAME: "catalog",
  R2_SQL_NAMESPACE: "pc_keiba",
  R2_SQL_TOKEN: "token",
};

const filters: WinRateHeatmapStatsFilters = {
  date: "20260923",
  includeDistance: true,
  includeSurface: false,
  includeTurn: false,
  includeVenue: true,
  keibajoCode: "36",
  raceBango: "09",
  source: "nar",
  years: 10,
};

it("aggregates top-3 finishers of the matched races for the target averages", () => {
  const sql = buildConditionCorrelationTargetQuery(env, filters);
  expect(sql.includes("count(*) AS top3_count")).toBe(true);
  expect(sql.includes("AS win_count")).toBe(true);
  expect(sql.includes("avg(popularity) AS average_popularity")).toBe(true);
  expect(sql.includes("avg(odds) AS average_odds")).toBe(true);
  expect(sql.includes("<= 3")).toBe(true);
  expect(sql.includes("FROM pc_keiba.nvd_se se")).toBe(true);
});

it("reads the current race runners with popularity and odds", () => {
  const sql = buildConditionCorrelationEntriesQuery(env, filters);
  expect(sql.includes("FROM pc_keiba.nvd_se se")).toBe(true);
  expect(sql.includes("race_bango = '09'")).toBe(true);
  expect(sql.includes("AS horse_name")).toBe(true);
  expect(sql.includes("/ 10.0 AS odds")).toBe(true);
});

it("reads career counts before the race date for horses, jockeys, trainers and owners", () => {
  const sql = buildConditionCorrelationCareerQuery(env, filters);
  expect(sql.includes("concat(se.kaisai_nen, se.kaisai_tsukihi) < '20260923'")).toBe(true);
  expect(sql.includes("'horse' AS kind")).toBe(true);
  expect(sql.includes("'jockey' AS kind")).toBe(true);
  expect(sql.includes("'trainer' AS kind")).toBe(true);
  expect(sql.includes("'owner' AS kind")).toBe(true);
  expect(sql.includes("regexp_match(ketto_toroku_bango, '^0+$') IS NULL")).toBe(true);
  expect(sql.split("UNION ALL").length).toBe(4);
});

it("scores runners without targets or careers as neutral and orders ties by horse number", () => {
  const rows = composeConditionCorrelationRows({
    careerRows: [{ entity: "x", kind: "unknown", show_count: 1, starts: 1, win_count: 1 }],
    entryRows: [
      { horse_name: "", umaban: "10" },
      { horse_name: "アルファ", jockey_name: 7, umaban: "00", popularity: "abc", odds: null },
    ],
    targetRows: [],
  });
  expect(rows.map((row) => [row.horseNumber, row.horseName, row.score])).toStrictEqual([
    ["0", "アルファ", 0.5],
    ["10", "-", 0.5],
  ]);
  expect(rows[0]?.details.map((detail) => [detail.key, detail.value, detail.target])).toStrictEqual(
    [
      ["horseShow", null, null],
      ["horseWin", null, null],
      ["jockeyShow", null, null],
      ["trainerShow", null, null],
      ["ownerShow", null, null],
      ["popularity", null, null],
      ["odds", null, null],
    ],
  );
});

it("orders runners by score descending before horse number", () => {
  const rows = composeConditionCorrelationRows({
    careerRows: [
      { entity: "strong", kind: "jockey", show_count: "90", starts: "100", win_count: "40" },
      { entity: "weak", kind: "jockey", show_count: 10, starts: 100, win_count: 1 },
    ],
    entryRows: [
      { horse_name: "弱", jockey_name: "weak", umaban: "01" },
      { horse_name: "強", jockey_name: "strong", umaban: "02" },
    ],
    targetRows: [{ average_odds: null, average_popularity: "", top3_count: 0, win_count: 0 }],
  });
  expect(rows.map((row) => [row.horseNumber, row.score])).toStrictEqual([
    ["2", 0.56],
    ["1", 0.44],
  ]);
});

it("uses the popularity and odds floors when the target averages are small", () => {
  const rows = composeConditionCorrelationRows({
    careerRows: [],
    entryRows: [{ horse_name: "馬", odds: 30, popularity: 12, umaban: "03" }],
    targetRows: [{ average_odds: 2, average_popularity: 1, top3_count: 3, win_count: 1 }],
  });
  expect(rows[0]?.details.map((detail) => [detail.key, detail.score])).toStrictEqual([
    ["horseShow", 0.5],
    ["horseWin", 0.5],
    ["jockeyShow", 0.5],
    ["trainerShow", 0.5],
    ["ownerShow", 0.5],
    ["popularity", 0],
    ["odds", 0],
  ]);
});
