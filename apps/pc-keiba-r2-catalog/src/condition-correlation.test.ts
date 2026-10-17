// Run with bun (bunx vitest).
import { expect, it } from "vitest";

import {
  buildConditionCorrelationDayCareerQuery,
  buildConditionCorrelationEntriesQuery,
  buildConditionCorrelationTargetQuery,
  composeConditionCorrelationRows,
  dayCareerCoversEntries,
  normaliseDayCareerRows,
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

it("reads one day of career counts in a single grouped scan of the source history", () => {
  const sql = buildConditionCorrelationDayCareerQuery(env, filters);
  expect(sql).toMatch(
    "FROM pc_keiba.nvd_se\n  WHERE kaisai_nen = '2026'\n    AND kaisai_tsukihi = '0923'",
  );
  expect(sql).toMatch("concat(se.kaisai_nen, se.kaisai_tsukihi) < '20260923'");
  expect(sql).toMatch("regexp_match(ketto_toroku_bango, '^0+$') IS NULL");
  expect(sql).toMatch("LEFT JOIN day_horses h ON h.horse_key = ch.ketto_toroku_bango");
  expect(sql).toMatch("LEFT JOIN day_owners o ON o.owner_key = ch.owner_name");
  expect(sql).toMatch(
    "GROUP BY GROUPING SETS ((horse_key), (jockey_key), (trainer_key), (owner_key))",
  );
  expect(sql).toMatch(
    "SELECT CAST(NULL AS VARCHAR), jockey_key, CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT) FROM day_jockeys",
  );
  expect(sql).not.toMatch("race_bango");
  expect(sql.match(/FROM career_history/gu)).toStrictEqual(["FROM career_history"]);
});

it("splits day career rows into requested entities and per-entity counts", () => {
  expect(
    normaliseDayCareerRows([
      { horse_key: "2020100001", show_count: 3, starts: 10, win_count: 1 },
      { jockey_key: "騎手A", show_count: null, starts: null, win_count: null },
      {
        horse_key: null,
        jockey_key: null,
        owner_key: null,
        show_count: 9,
        starts: 99,
        win_count: 2,
      },
      { owner_key: "馬主B", show_count: "4", starts: "8", win_count: "2" },
      { horse_key: "2020100001", starts: undefined },
    ]),
  ).toStrictEqual({
    requested: ["jockey\u0000騎手A", "horse\u00002020100001"],
    rows: [
      { entity: "2020100001", kind: "horse", show_count: 3, starts: 10, win_count: 1 },
      { entity: "馬主B", kind: "owner", show_count: "4", starts: "8", win_count: "2" },
    ],
  });
});

it("covers a race only when every runner's horse, jockey, trainer and owner was requested", () => {
  const career = {
    requested: [
      "horse\u00002020100001",
      "jockey\u0000騎手A",
      "trainer\u0000調教師A",
      "owner\u0000馬主A",
    ],
    rows: [],
  };
  const entry = {
    jockey_name: "騎手A",
    ketto_toroku_bango: "2020100001",
    owner_name: "馬主A",
    trainer_name: "調教師A",
  };
  expect(dayCareerCoversEntries(career, [entry])).toBe(true);
  expect(dayCareerCoversEntries(career, [{ ...entry, ketto_toroku_bango: "0000000000" }])).toBe(
    true,
  );
  expect(dayCareerCoversEntries(career, [{ ...entry, ketto_toroku_bango: "" }])).toBe(true);
  expect(dayCareerCoversEntries(career, [{ ...entry, jockey_name: "騎手B" }])).toBe(false);
  expect(dayCareerCoversEntries(career, [{ ...entry, ketto_toroku_bango: "2021100002" }])).toBe(
    false,
  );
  expect(dayCareerCoversEntries({ requested: [], rows: [] }, [])).toBe(true);
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

it.each([
  { venue: "05", odds: 8.5, popularity: 3.1 },
  { venue: "08", odds: 15.9, popularity: 4.2 },
])("neutralizes saved $venue-11 market sentinels without changing available values", (sample) => {
  // Saved all-jra-score-check/05-11.json and 08-11.json market target shapes.
  const rows = composeConditionCorrelationRows({
    careerRows: [],
    entryRows: [
      { umaban: "01", odds: "0000", popularity: "00" },
      { umaban: "02", odds: 0, popularity: 0 },
      { umaban: "03", odds: sample.odds, popularity: sample.popularity },
    ],
    targetRows: [{ average_odds: sample.odds, average_popularity: sample.popularity }],
  });
  expect(rows.map((row) => [row.horseNumber, row.score])).toStrictEqual([
    ["3", 0.63],
    ["1", 0.5],
    ["2", 0.5],
  ]);
  expect(rows[1]?.details.slice(-2).map(({ value, score }) => ({ value, score }))).toStrictEqual([
    { value: null, score: 0.5 },
    { value: null, score: 0.5 },
  ]);
  expect(rows[0]?.details.slice(-2)).toMatchObject([
    { value: sample.popularity, target: sample.popularity, score: 1 },
    { value: sample.odds, target: sample.odds, score: 1 },
  ]);
});

it("neutralizes zero market targets while preserving measured zero career rates", () => {
  const rows = composeConditionCorrelationRows({
    careerRows: [{ kind: "jockey", entity: "J", starts: 10, win_count: 0, show_count: 0 }],
    entryRows: [{ umaban: "01", jockey_name: "J", odds: 8.5, popularity: 3 }],
    targetRows: [{ average_odds: 0, average_popularity: "00" }],
  });
  expect(rows[0]?.details.slice(-2)).toMatchObject([
    { value: 3, target: null, score: 0.5 },
    { value: 8.5, target: null, score: 0.5 },
  ]);
  expect(rows[0]?.details.find((detail) => detail.key === "jockeyShow")).toMatchObject({
    value: 0,
    score: 0,
  });
});

it.each(["jra", "nar"] satisfies Array<WinRateHeatmapStatsFilters["source"]>)(
  "excludes zero sentinels before averaging and scaling %s market values",
  (source) => {
    const target = buildConditionCorrelationTargetQuery(env, { ...filters, source });
    const entries = buildConditionCorrelationEntriesQuery(env, { ...filters, source });
    expect(target).toMatch(/nullif\(try_cast\([^\n]+tansho_ninkijun[^\n]+, 0\) AS popularity/u);
    expect(target).toMatch(/nullif\(try_cast\([^\n]+tansho_odds[^\n]+, 0\) \/ 10\.0 AS odds/u);
    expect(entries).toMatch(/nullif\(try_cast\([^\n]+tansho_ninkijun[^\n]+, 0\) AS popularity/u);
    expect(entries).toMatch(/nullif\(try_cast\([^\n]+tansho_odds[^\n]+, 0\) \/ 10\.0 AS odds/u);
  },
);
