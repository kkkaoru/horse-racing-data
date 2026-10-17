// Run with bun via Vitest; local DuckDB fixtures do not access production.
import { expect, it } from "vitest";
import { DuckDBInstance } from "@duckdb/node-api";
import {
  buildWinRateHeatmapBloodlineQuery,
  buildWinRateHeatmapSimilarQuery,
} from "./win-rate-heatmap-stats";

import {
  buildConditionFinishPositionStatsQuery,
  buildConditionFrameStatsQuery,
  buildConditionRaceTimeStatsQuery,
  buildConditionTargetRacesQuery,
  buildConditionWeightClassStatsQuery,
  normaliseTargetRaceRow,
  isBanEiKeibajo,
  aggregateFinishPositionStats,
  normaliseConditionHistoryStatsPayload,
  normaliseRaceTimeStats,
  normaliseWeightClassRow,
} from "./condition-history-stats";
import type { R2SqlCatalogConfig, WinRateHeatmapStatsFilters } from "./types";

const config: R2SqlCatalogConfig = {
  R2_SQL_ACCOUNT_ID: "account",
  R2_SQL_BUCKET_NAME: "bucket",
  R2_SQL_NAMESPACE: "pc_keiba",
  R2_SQL_TOKEN: "token",
};

const jraFilters: WinRateHeatmapStatsFilters = {
  date: "20260715",
  includeDistance: true,
  includeSurface: true,
  includeTurn: true,
  includeVenue: true,
  keibajoCode: "05",
  raceBango: "01",
  source: "jra",
  years: 10,
};

it("builds frame-stats SQL with heatmap history filters and empty details", () => {
  const sql = buildConditionFrameStatsQuery(config, jraFilters);
  expect(sql).toMatch("current_frames AS");
  expect(sql).toMatch("AS frame_number");
  expect(sql).toMatch("approx_percentile_cont(finish_position, 0.5)");
  expect(sql).toMatch("AND ra.keibajo_code = '05'");
  expect(sql).toMatch("cr.kyori_int IS NOT NULL");
  expect(sql).toMatch("THEN '芝'");
  expect(sql).toMatch("THEN '左'");
  expect(sql).not.toMatch("jsonb_agg");
  expect(sql).not.toMatch("jvd_hr");
  expect(sql).not.toMatch("nvd_se");
});

it("unions JRA and NAR frame history when venue matching is off", () => {
  const sql = buildConditionFrameStatsQuery(config, { ...jraFilters, includeVenue: false });
  expect(sql).toMatch("FROM pc_keiba.jvd_se se");
  expect(sql).toMatch("FROM pc_keiba.nvd_se se");
  expect(sql).toMatch("UNION ALL");
  expect(sql).not.toMatch("AND ra.keibajo_code = '05'");
});

it("builds body and carried weight class SQL with Ban'ei venue excluded", () => {
  const body = buildConditionWeightClassStatsQuery({
    env: config,
    filters: jraFilters,
    kind: "body",
  });
  const carried = buildConditionWeightClassStatsQuery({
    env: config,
    filters: jraFilters,
    kind: "carried",
  });
  expect(body).toMatch("AS body_weight");
  expect(body).toMatch("'le399'");
  expect(body).toMatch("'ge540'");
  expect(body).toMatch("NOT IN ('81', '82', '83', '84')");
  expect(carried).toMatch("AS carried_weight");
  expect(carried).toMatch("'le49'");
  expect(carried).toMatch("'57.5-59'");
  expect(carried).toMatch("/ 10.0");
  expect(body).not.toMatch("jsonb_agg");
  expect(carried).not.toMatch("jsonb_agg");
});

it("builds Ban-ei body-weight class SQL with hex decode and Ban-ei buckets", () => {
  const body = buildConditionWeightClassStatsQuery({
    env: config,
    filters: { ...jraFilters, keibajoCode: "83", source: "nar" },
    kind: "body",
  });
  expect(body).toMatch(
    "(strpos('0123456789ABCDEF', substr(upper(btrim(coalesce(se.bataiju, ''))), 3, 1)) - 1) * 1",
  );
  expect(body).not.toMatch("0x");
  expect(body).toMatch("'le899'");
  expect(body).toMatch("'ge1200'");
  expect(body).not.toMatch("NOT IN ('81', '82', '83', '84')");
  expect(body).toMatch("AND se.keibajo_code IN ('81', '82', '83', '84')");
});

it("builds decimal weight-change class SQL excluding debut and unmeasured changes", () => {
  const change = buildConditionWeightClassStatsQuery({
    env: config,
    filters: jraFilters,
    kind: "change",
  });
  expect(change).toMatch("AS weight_change");
  expect(change).toMatch(
    "CASE WHEN btrim(coalesce(se.zogen_fugo, '')) = '-' THEN -(try_cast(upper(btrim(coalesce(se.zogen_sa, ''))) AS INT)) ELSE (try_cast(upper(btrim(coalesce(se.zogen_sa, ''))) AS INT)) END",
  );
  expect(change).toMatch("NOT IN ('', '999')");
  expect(change).toMatch("WHEN weight_change >= 20 THEN 'plus20'");
  expect(change).toMatch("WHEN weight_change >= -2 THEN 'even'");
  expect(change).toMatch("ELSE 'minus20'");
  expect(change).toMatch("NOT IN ('81', '82', '83', '84')");
  expect(change).not.toMatch("strpos");
});

it("builds Ban-ei weight-change class SQL with hex decode", () => {
  const change = buildConditionWeightClassStatsQuery({
    env: config,
    filters: { ...jraFilters, keibajoCode: "83", source: "nar" },
    kind: "change",
  });
  expect(change).toMatch(
    "(strpos('0123456789ABCDEF', substr(upper(btrim(coalesce(se.zogen_sa, ''))), 1, 1)) - 1) * 256",
  );
  expect(change).toMatch("NOT IN ('', 'FFF')");
  expect(change).toMatch("WHEN weight_change >= 10 THEN 'plus10'");
  expect(change).not.toMatch("NOT IN ('81', '82', '83', '84')");
  expect(change).toMatch("AND se.keibajo_code IN ('81', '82', '83', '84')");
});

it("builds finish-position horse-level SQL and race-time aggregate SQL without jsonb_agg", () => {
  const finish = buildConditionFinishPositionStatsQuery(config, {
    ...jraFilters,
    includeDistance: false,
    includeSurface: false,
    includeTurn: false,
    source: "nar",
    keibajoCode: "83",
    years: 5,
  });
  const time = buildConditionRaceTimeStatsQuery(config, jraFilters);
  expect(finish).toMatch("IN (1, 2, 3, 4, 5)");
  expect(finish).toMatch("FROM pc_keiba.nvd_se se");
  expect(finish).toMatch("AS bamei");
  expect(finish).toMatch("AS win_odds");
  expect(finish).not.toMatch("THEN '芝'");
  expect(finish).not.toMatch("jsonb_agg");
  expect(finish).not.toMatch("GROUP BY finish_position");
  expect(finish).toMatch("WHEN cr.kyori_int < 1400 THEN 'sprint'");
  expect(finish).toMatch("WHEN cr.kyori_int <= 1500 THEN 'extended_sprint'");
  expect(time).toMatch("AS race_count");
  expect(time).toMatch("AS INT) = 1");
  expect(time).toMatch("AS INT) = cr.kyori_int");
  expect(time).toMatch("approx_percentile_cont(race_time, 0.5)");
  expect(time).toMatch("WHEN cr.kyori_int < 1200 THEN 'sprint'");
  expect(time).not.toMatch("scaled_race_time");
  expect(time).not.toMatch("jsonb_agg");
});

it("builds winner target-race list SQL without jsonb_agg", () => {
  const sql = buildConditionTargetRacesQuery(config, jraFilters);
  expect(sql).toMatch("AS target_race_date");
  expect(sql).toMatch("AS INT) = 1");
  expect(sql).toMatch("AS INT) = cr.kyori_int");
  expect(sql).toMatch("LIMIT 500");
  expect(sql).toMatch("FROM pc_keiba.jvd_se se");
  expect(sql).not.toMatch("jsonb_agg");
});

it("keeps exact kyori on race-time clocks when includeDistance is off", () => {
  const time = buildConditionRaceTimeStatsQuery(config, {
    ...jraFilters,
    includeDistance: false,
  });
  const targets = buildConditionTargetRacesQuery(config, {
    ...jraFilters,
    includeDistance: false,
  });
  const finish = buildConditionFinishPositionStatsQuery(config, {
    ...jraFilters,
    includeDistance: false,
  });
  expect(time).toMatch("AS INT) = cr.kyori_int");
  expect(targets).toMatch("AS INT) = cr.kyori_int");
  expect(finish).not.toMatch("AS INT) = cr.kyori_int");
  expect(finish).toMatch("WHEN cr.kyori_int < 1200 THEN 'sprint'");
});

it("skips includeGrade matching for unlisted current grades in condition history", () => {
  const sql = buildConditionFinishPositionStatsQuery(config, {
    ...jraFilters,
    includeGrade: true,
  });
  expect(sql).toMatch("END NOT IN ('A', 'B', 'C', 'D', 'F', 'G', 'H', 'L', 'S')");
});

it("applies class, age, condition-key, race-title, and ungraded-open filters to condition history", () => {
  const sql = buildConditionFrameStatsQuery(config, {
    ...jraFilters,
    includeAge: true,
    includeClass: true,
    includeConditionKey: true,
    includeRaceTitle: true,
  });
  expect(sql).toMatch(
    "btrim(coalesce(ra.kyoso_joken_code, '')) = btrim(coalesce(cr.kyoso_joken_code, ''))",
  );
  expect(sql).toMatch(
    "btrim(coalesce(ra.kyoso_shubetsu_code, '')) = btrim(coalesce(cr.kyoso_shubetsu_code, ''))",
  );
  expect(sql).toMatch("THEN '1勝クラス'");
  expect(sql).toMatch("IN ('A', 'F')");
  expect(sql).toMatch("btrim(coalesce(cr.kyoso_joken_code, '')) <> '999'");
});

it("maps a target-race winner row onto the viewer camelCase shape", () => {
  expect(
    normaliseTargetRaceRow({
      bamei: "イクイノックス",
      jockey_name: "ルメール",
      keibajo_code: "05",
      kohan_3f: 351,
      owner_name: "シルク",
      popularity: "01",
      race_bango: 8,
      race_name: "天皇賞",
      race_time: 1450,
      target_race_date: "20241027",
      trainer_name: "堀",
      umaban: "05",
    }),
  ).toStrictEqual({
    date: "20241027",
    horseName: "イクイノックス",
    horseNumber: "05",
    jockeyName: "ルメール",
    keibajoCode: "05",
    kohan3f: "351",
    kyori: "",
    ownerName: "シルク",
    popularity: "01",
    raceName: "天皇賞",
    raceNumber: "8",
    raceTime: "1450",
    trainerName: "堀",
  });
});

it("rejects unsafe namespace and invalid heatmap filters", () => {
  expect(() =>
    buildConditionFrameStatsQuery({ ...config, R2_SQL_NAMESPACE: "pc_keiba;drop" }, jraFilters),
  ).toThrow("R2_SQL_NAMESPACE must be an unquoted SQL identifier");
  expect(() =>
    buildConditionWeightClassStatsQuery({
      env: config,
      filters: { ...jraFilters, date: "20260231" },
      kind: "body",
    }),
  ).toThrow("date must be a valid calendar date");
  expect(() => buildConditionFinishPositionStatsQuery(config, { ...jraFilters, years: 0 })).toThrow(
    "years must be an integer from 1 to 50",
  );
  expect(() =>
    buildConditionRaceTimeStatsQuery(config, { ...jraFilters, keibajoCode: "5" }),
  ).toThrow("keibajoCode must contain two uppercase alphanumeric characters");
});

it("identifies Ban'ei venues for omitted carried-weight stats", () => {
  expect(isBanEiKeibajo("81")).toBe(true);
  expect(isBanEiKeibajo("82")).toBe(true);
  expect(isBanEiKeibajo("83")).toBe(true);
  expect(isBanEiKeibajo("84")).toBe(true);
  expect(isBanEiKeibajo("05")).toBe(false);
  expect(isBanEiKeibajo("08")).toBe(false);
});

it("maps weight-class rates from counts including a zero-start class", () => {
  expect(
    normaliseWeightClassRow({
      class_key: "480-499",
      quinella_count: "2",
      show_count: 3n,
      starts: 10,
      win_count: 1,
    }),
  ).toStrictEqual({
    key: "480-499",
    quinellaCount: 2,
    quinellaRate: 20,
    showCount: 3,
    showRate: 30,
    starts: 10,
    winCount: 1,
    winRate: 10,
  });
  expect(
    normaliseWeightClassRow({
      class_key: 12n,
      quinella_count: 0,
      show_count: 0,
      starts: 0,
      win_count: 0,
    }).key,
  ).toBe("12");
  expect(
    normaliseWeightClassRow({
      class_key: true,
      quinella_count: 0,
      show_count: 0,
      starts: 0,
      win_count: 0,
    }),
  ).toStrictEqual({
    key: "true",
    quinellaCount: 0,
    quinellaRate: 0,
    showCount: 0,
    showRate: 0,
    starts: 0,
    winCount: 0,
    winRate: 0,
  });
});

it("aggregates horse-level finish rows into counted stats with details", () => {
  expect(
    aggregateFinishPositionStats([
      {
        bamei: "先行馬",
        finish_position: 1,
        jockey_name: "武豊",
        kakutei_chakujun: "01",
        keibajo_code: "07",
        odds: 4.1,
        popularity: "02",
        popularity_number: 2,
        race_bango: "01",
        race_name: "一般競走",
        soha_time: "1207",
        target_race_date: "20260726",
        umaban: "08",
        wakuban: "08",
        win_odds: "41",
      },
      {
        bamei: "後続馬",
        finish_position: 1,
        jockey_name: "ルメール",
        kakutei_chakujun: "01",
        keibajo_code: "07",
        odds: 8.7,
        popularity: "04",
        popularity_number: 4,
        race_bango: "02",
        race_name: "一般競走",
        soha_time: "1211",
        target_race_date: "20250823",
        umaban: "03",
        wakuban: "03",
        win_odds: "87",
      },
    ]),
  ).toStrictEqual([
    {
      averageOdds: 6.4,
      averagePopularity: 3,
      count: 2,
      details: [
        {
          date: "20260726",
          frameNumber: "08",
          horseName: "先行馬",
          horseNumber: "08",
          jockeyName: "武豊",
          keibajoCode: "07",
          popularity: "02",
          raceName: "一般競走",
          raceNumber: "01",
          raceTime: "1207",
          rank: "01",
          winOdds: "41",
        },
        {
          date: "20250823",
          frameNumber: "03",
          horseName: "後続馬",
          horseNumber: "03",
          jockeyName: "ルメール",
          keibajoCode: "07",
          popularity: "04",
          raceName: "一般競走",
          raceNumber: "02",
          raceTime: "1211",
          rank: "01",
          winOdds: "87",
        },
      ],
      finishPosition: 1,
      medianOdds: 6.4,
      medianPopularity: 3,
    },
  ]);
});

it("maps race-time scalars and returns zeros when the query is empty", () => {
  expect(normaliseRaceTimeStats(undefined, [], [])).toStrictEqual({
    averageKohan3f: null,
    averageRaceTime: null,
    correlationRows: [],
    fastestDetail: null,
    fastestKohan3f: null,
    fastestRaceTime: null,
    medianKohan3f: null,
    medianRaceTime: null,
    raceCount: 0,
    targetRaces: [],
  });
  expect(
    normaliseRaceTimeStats(
      {
        average_kohan_3f: "35.15",
        average_race_time: 1345.44,
        fastest_kohan_3f: 34,
        fastest_race_time: "1330",
        median_kohan_3f: Number.NaN,
        median_race_time: {},
        race_count: "8",
      },
      [],
      [],
    ),
  ).toStrictEqual({
    averageKohan3f: 35.2,
    averageRaceTime: 1345.4,
    correlationRows: [],
    fastestDetail: null,
    fastestKohan3f: 34,
    fastestRaceTime: 1330,
    medianKohan3f: null,
    medianRaceTime: null,
    raceCount: 8,
    targetRaces: [],
  });
});

it("normalises the combined payload and min-max frame scores", () => {
  expect(
    normaliseConditionHistoryStatsPayload({
      correlationRows: [],
      carriedRows: [
        {
          class_key: "55.5-57",
          quinella_count: 1,
          show_count: 1,
          starts: 2,
          win_count: 1,
        },
      ],
      finishRows: [
        {
          bamei: "テスト",
          finish_position: 1,
          jockey_name: "武豊",
          kakutei_chakujun: "01",
          keibajo_code: "05",
          odds: 2,
          popularity: "01",
          popularity_number: 1,
          race_bango: "08",
          race_name: "一般競走",
          soha_time: "1330",
          target_race_date: "20260715",
          umaban: "01",
          wakuban: "01",
          win_odds: "20",
        },
      ],
      frameRows: [
        {
          average_finish: 1,
          average_popularity: 2,
          count: 10,
          frame_number: 1,
          median_finish: 1,
          median_popularity: 2,
          quinella_count: 4,
          runner_count: 16,
          show_count: 5,
          win_count: 2,
        },
        {
          average_finish: 4,
          average_popularity: 8,
          count: 10,
          frame_number: "8",
          median_finish: 4,
          median_popularity: 8,
          quinella_count: 1,
          runner_count: null,
          show_count: 2,
          win_count: 0,
        },
      ],
      raceTimeRows: [{ race_count: 3, average_race_time: 1400 }],
      targetRaceRows: [],
      weightChangeRows: [
        {
          class_key: "plus2",
          quinella_count: 1,
          show_count: 2,
          starts: 4,
          win_count: 1,
        },
      ],
      weightRows: [
        {
          class_key: "480-499",
          quinella_count: 0,
          show_count: 0,
          starts: 1,
          win_count: 0,
        },
      ],
    }),
  ).toStrictEqual({
    carriedWeightClassStats: [
      {
        key: "55.5-57",
        quinellaCount: 1,
        quinellaRate: 50,
        showCount: 1,
        showRate: 50,
        starts: 2,
        winCount: 1,
        winRate: 50,
      },
    ],
    finishPositionStats: [
      {
        averageOdds: 2,
        averagePopularity: 1,
        count: 1,
        details: [
          {
            date: "20260715",
            frameNumber: "01",
            horseName: "テスト",
            horseNumber: "01",
            jockeyName: "武豊",
            keibajoCode: "05",
            popularity: "01",
            raceName: "一般競走",
            raceNumber: "08",
            raceTime: "1330",
            rank: "01",
            winOdds: "20",
          },
        ],
        finishPosition: 1,
        medianOdds: 2,
        medianPopularity: 1,
      },
    ],
    frameStats: [
      {
        averageFinish: 1,
        averagePopularity: 2,
        count: 10,
        details: [],
        frameNumber: "1",
        medianFinish: 1,
        medianPopularity: 2,
        quinellaCount: 4,
        quinellaRate: 40,
        runnerCount: 16,
        score: 1,
        showCount: 5,
        showRate: 50,
        winCount: 2,
        winRate: 20,
      },
      {
        averageFinish: 4,
        averagePopularity: 8,
        count: 10,
        details: [],
        frameNumber: "8",
        medianFinish: 4,
        medianPopularity: 8,
        quinellaCount: 1,
        quinellaRate: 10,
        runnerCount: null,
        score: 0,
        showCount: 2,
        showRate: 20,
        winCount: 0,
        winRate: 0,
      },
    ],
    raceTimeStats: {
      averageKohan3f: null,
      averageRaceTime: 1400,
      correlationRows: [],
      fastestDetail: null,
      fastestKohan3f: null,
      fastestRaceTime: null,
      medianKohan3f: null,
      medianRaceTime: null,
      raceCount: 3,
      targetRaces: [],
    },
    weightChangeClassStats: [
      {
        key: "plus2",
        quinellaCount: 1,
        quinellaRate: 25,
        showCount: 2,
        showRate: 50,
        starts: 4,
        winCount: 1,
        winRate: 25,
      },
    ],
    weightClassStats: [
      {
        key: "480-499",
        quinellaCount: 0,
        quinellaRate: 0,
        showCount: 0,
        showRate: 0,
        starts: 1,
        winCount: 0,
        winRate: 0,
      },
    ],
  });
});

it("assigns equal positive frame scores to 1 and zero scores to 0", () => {
  expect(
    normaliseConditionHistoryStatsPayload({
      correlationRows: [],
      carriedRows: [],
      finishRows: [],
      frameRows: [
        {
          average_finish: 2,
          average_popularity: 1,
          count: 2,
          frame_number: "1",
          median_finish: 2,
          median_popularity: 1,
          quinella_count: 0,
          show_count: 0,
          win_count: 0,
        },
        {
          average_finish: 2,
          average_popularity: 1,
          count: 2,
          frame_number: "2",
          median_finish: 2,
          median_popularity: 1,
          quinella_count: 0,
          show_count: 0,
          win_count: 0,
        },
      ],
      raceTimeRows: [],
      targetRaceRows: [],
      weightChangeRows: [],
      weightRows: [],
    }).frameStats,
  ).toStrictEqual([
    {
      averageFinish: 2,
      averagePopularity: 1,
      count: 2,
      details: [],
      frameNumber: "1",
      medianFinish: 2,
      medianPopularity: 1,
      quinellaCount: 0,
      quinellaRate: 0,
      runnerCount: null,
      score: 1,
      showCount: 0,
      showRate: 0,
      winCount: 0,
      winRate: 0,
    },
    {
      averageFinish: 2,
      averagePopularity: 1,
      count: 2,
      details: [],
      frameNumber: "2",
      medianFinish: 2,
      medianPopularity: 1,
      quinellaCount: 0,
      quinellaRate: 0,
      runnerCount: null,
      score: 1,
      showCount: 0,
      showRate: 0,
      winCount: 0,
      winRate: 0,
    },
  ]);
  expect(
    normaliseConditionHistoryStatsPayload({
      correlationRows: [],
      carriedRows: [],
      finishRows: [],
      frameRows: [
        {
          average_finish: 0,
          average_popularity: null,
          count: 1,
          frame_number: "3",
          median_finish: 0,
          median_popularity: "",
          quinella_count: 0,
          runner_count: "nope",
          show_count: 0,
          win_count: 0,
        },
      ],
      raceTimeRows: [],
      targetRaceRows: [],
      weightChangeRows: [],
      weightRows: [],
    }).frameStats,
  ).toStrictEqual([
    {
      averageFinish: 0,
      averagePopularity: null,
      count: 1,
      details: [],
      frameNumber: "3",
      medianFinish: 0,
      medianPopularity: null,
      quinellaCount: 0,
      quinellaRate: 0,
      runnerCount: null,
      score: 0,
      showCount: 0,
      showRate: 0,
      winCount: 0,
      winRate: 0,
    },
  ]);
});

it("rejects incomplete aggregate rows", () => {
  expect(() =>
    normaliseWeightClassRow({
      class_key: "",
      quinella_count: 0,
      show_count: 0,
      starts: 1,
      win_count: 0,
    }),
  ).toThrow("R2 SQL row is missing class_key");
  expect(aggregateFinishPositionStats([{ finish_position: 0 }])).toStrictEqual([]);
  expect(() =>
    normaliseRaceTimeStats(
      {
        race_count: 2n ** 1024n,
      },
      [],
      [],
    ),
  ).toThrow("R2 SQL row is missing race_count");
  expect(() =>
    normaliseConditionHistoryStatsPayload({
      correlationRows: [],
      carriedRows: [],
      finishRows: [],
      frameRows: [
        {
          average_finish: 1,
          count: 1,
          frame_number: {},
          median_finish: 1,
          quinella_count: 0,
          show_count: 0,
          win_count: 0,
        },
      ],
      raceTimeRows: [],
      targetRaceRows: [],
      weightChangeRows: [],
      weightRows: [],
    }),
  ).toThrow("R2 SQL row is missing frame_number");
});

it("uses mapped gates on both sides and never pools domestic frames for overseas", () => {
  const sql = buildConditionFrameStatsQuery(config, {
    ...jraFilters,
    keibajoCode: "A8",
    includeVenue: false,
  });
  expect(sql).toMatch("SELECT DISTINCT gate_mapping.gate_number AS frame_number");
  expect(sql).toMatch(
    /current_frames AS \([\s\S]*?WHERE se\.kaisai_nen = '[0-9]{4}'\s+AND se\.kaisai_tsukihi = '[0-9]{4}'\s+AND se\.keibajo_code = 'A8'\s+AND se\.race_bango = '[0-9]{2}'/u,
  );
  expect(sql).toMatch("cf.frame_number = gate_mapping.gate_number");
  expect(sql).toMatch("AND se.keibajo_code = cr.keibajo_code");
  expect(sql).toMatch("gate_mapping.source = 'netkeiba'");
  expect(sql).toMatch("gate_mapping.umaban = se.umaban");
  expect(sql).toMatch("gate_mapping.gate_number BETWEEN 1 AND 32767");
  expect(sql).not.toMatch("se.wakuban");
  expect(sql).not.toMatch("nvd_se");
});

it("keeps non-rate queries positive-only and domestic rate SQL unchanged", () => {
  const filters: WinRateHeatmapStatsFilters = {
    ...jraFilters,
    date: "20261004",
    keibajoCode: "A8",
    raceBango: "05",
  };
  expect(buildConditionFinishPositionStatsQuery(config, filters)).not.toMatch("ijo_kubun_code");
  expect(buildConditionRaceTimeStatsQuery(config, filters)).not.toMatch("ijo_kubun_code");
  expect(buildConditionTargetRacesQuery(config, filters)).not.toMatch("ijo_kubun_code");
  expect(buildConditionFrameStatsQuery(config, jraFilters)).not.toMatch("ijo_kubun_code");
  expect(buildConditionFrameStatsQuery(config, jraFilters)).toMatch("avg(finish_position)");
  expect(
    buildConditionWeightClassStatsQuery({ env: config, filters: jraFilters, kind: "body" }),
  ).toMatch("WHEN finish_position <= 2 THEN");
});

it("executes Arc native DNF/DSQ denominators without inventing placed horses or zero finishing places", async () => {
  const instance = await DuckDBInstance.create(":memory:");
  const connection = await instance.connect();
  const filters: WinRateHeatmapStatsFilters = {
    ...jraFilters,
    date: "20261004",
    keibajoCode: "A8",
    raceBango: "05",
    includeVenue: false,
    includeDistance: false,
    includeSurface: false,
    includeTurn: false,
  };
  try {
    await connection.run(`CREATE SCHEMA pc_keiba;
      CREATE MACRO btrim(v) AS trim(v);
      CREATE MACRO regexp_match(v,p) AS CASE WHEN regexp_matches(v,p) THEN [v] ELSE NULL END;
      CREATE MACRO approx_percentile_cont(v,p) AS quantile_cont(v,p);
      CREATE TABLE pc_keiba.jvd_ra AS SELECT y AS kaisai_nen, '1004' AS kaisai_tsukihi,
        'A8' AS keibajo_code, '05' AS race_bango, '凱旋門賞' AS kyosomei_hondai,
        '11' AS track_code, 'A' AS grade_code, '14' AS kyoso_shubetsu_code,
        '999' AS kyoso_joken_code, '' AS kyoso_joken_meisho, '2400' AS kyori, '8' AS shusso_tosu
        FROM (VALUES ('2025'), ('2026')) t(y);
      CREATE TABLE pc_keiba.jvd_se AS SELECT '2025' AS kaisai_nen, '1004' AS kaisai_tsukihi,
        'A8' AS keibajo_code, '05' AS race_bango, num AS umaban,
        '2020100001' AS ketto_toroku_bango, rank AS kakutei_chakujun, code AS ijo_kubun_code,
        'Known jockey' AS kishumei_ryakusho, 'Known trainer' AS chokyoshimei_ryakusho,
        '500' AS bataiju, '580' AS futan_juryo, '00' AS wakuban, '01' AS tansho_ninkijun,
        'Horse' AS bamei, '2300' AS soha_time, '100' AS tansho_odds
        FROM (VALUES ('01','01','0'), ('02','02','0'), ('03','00','4'), ('04','00','5'),
          ('05','01','1'), ('06','02','2'), ('07','00','0'), ('08',NULL,'9')) t(num,rank,code);
      INSERT INTO pc_keiba.jvd_se SELECT * REPLACE ('2026' AS kaisai_nen) FROM pc_keiba.jvd_se WHERE umaban='01';
      CREATE TABLE pc_keiba.oversea_runner_source_id AS SELECT kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,umaban,
        'jra' AS race_source, 'netkeiba' AS source, '000a000001' AS source_horse_id, 16 AS gate_number FROM pc_keiba.jvd_se;
      CREATE TABLE pc_keiba.oversea_horse_pedigree (source VARCHAR, source_horse_id VARCHAR, sire_name VARCHAR, sire_sire_name VARCHAR, dam_sire_name VARCHAR);`);
    const columns: string = ["01", "02", "03", "05", "07", "09", "11", "13"]
      .map((n) => `ketto_joho_${n}b VARCHAR`)
      .join(", ");
    await connection.run(`CREATE TABLE pc_keiba.jvd_um (ketto_toroku_bango VARCHAR, ${columns});
      CREATE TABLE pc_keiba.nvd_um AS SELECT * FROM pc_keiba.jvd_um;
      CREATE TABLE pc_keiba.nvd_nu AS SELECT * FROM pc_keiba.jvd_um;
      INSERT INTO pc_keiba.jvd_um (ketto_toroku_bango,ketto_joho_01b) VALUES ('2020100001','Native sire');`);
    const frame = (
      await connection.runAndReadAll(buildConditionFrameStatsQuery(config, filters))
    ).getRowObjectsJson();
    expect(
      frame.map((r) => [
        r.frame_number,
        Number(r.count),
        Number(r.win_count),
        Number(r.quinella_count),
        Number(r.show_count),
        r.average_finish,
        r.median_finish,
      ]),
    ).toStrictEqual([[16, 4, 1, 2, 2, 1.5, 1.5]]);
    const body = (
      await connection.runAndReadAll(
        buildConditionWeightClassStatsQuery({ env: config, filters, kind: "body" }),
      )
    ).getRowObjectsJson();
    expect(
      body.map((r) => [
        r.class_key,
        Number(r.starts),
        Number(r.win_count),
        Number(r.quinella_count),
        Number(r.show_count),
      ]),
    ).toStrictEqual([["500-519", 4, 1, 2, 2]]);
    const carried = (
      await connection.runAndReadAll(
        buildConditionWeightClassStatsQuery({ env: config, filters, kind: "carried" }),
      )
    ).getRowObjectsJson();
    expect(
      carried.map((r) => [
        r.class_key,
        Number(r.starts),
        Number(r.win_count),
        Number(r.quinella_count),
        Number(r.show_count),
      ]),
    ).toStrictEqual([["57.5-59", 4, 1, 2, 2]]);
    const similar = (
      await connection.runAndReadAll(buildWinRateHeatmapSimilarQuery(config, filters))
    ).getRowObjectsJson();
    expect(
      similar.map((r) => [
        r.kind,
        Number(r.starts),
        Number(r.wins),
        Number(r.places),
        Number(r.shows),
      ]),
    ).toStrictEqual([
      ["jockey", 4, 1, 2, 2],
      ["trainer", 4, 1, 2, 2],
    ]);
    // Duplicate DNF rows must survive NULL/DISTINCT handling exactly once in bloodline history.
    await connection.run(
      "INSERT INTO pc_keiba.jvd_se SELECT * FROM pc_keiba.jvd_se WHERE umaban='03'",
    );
    const bloodline = (
      await connection.runAndReadAll(buildWinRateHeatmapBloodlineQuery(config, filters))
    ).getRowObjectsJson();
    expect(
      bloodline.map((r) => [
        r.category,
        Number(r.starts),
        Number(r.wins),
        Number(r.places),
        Number(r.shows),
      ]),
    ).toStrictEqual([["sire", 4, 1, 2, 2]]);
    const finish = (
      await connection.runAndReadAll(buildConditionFinishPositionStatsQuery(config, filters))
    ).getRowObjectsJson();
    expect(finish.map((r) => Number(r.finish_position))).toStrictEqual([1, 1, 2, 2]);
    // Even a malformed positive rank on a DNF/DSQ cannot manufacture a rate numerator.
    await connection.run(
      "UPDATE pc_keiba.jvd_se SET kakutei_chakujun='01' WHERE ijo_kubun_code IN ('4','5')",
    );
    const malformed = (
      await connection.runAndReadAll(buildWinRateHeatmapBloodlineQuery(config, filters))
    ).getRowObjectsJson();
    expect(
      malformed.map((r) => [Number(r.starts), Number(r.wins), Number(r.places), Number(r.shows)]),
    ).toStrictEqual([[4, 1, 2, 2]]);
    await connection.run(
      "DELETE FROM pc_keiba.jvd_se WHERE kaisai_nen='2025' AND ijo_kubun_code NOT IN ('4','5'); DELETE FROM pc_keiba.jvd_se WHERE umaban='03' AND kakutei_chakujun='01';",
    );
    const nonfinishOnly = (
      await connection.runAndReadAll(buildConditionFrameStatsQuery(config, filters))
    ).getRowObjectsJson();
    expect(
      nonfinishOnly.map((r) => [
        Number(r.count),
        Number(r.win_count),
        Number(r.quinella_count),
        Number(r.show_count),
        r.average_finish,
        r.median_finish,
      ]),
    ).toStrictEqual([[1, 0, 0, 0, null, null]]);
    // Known Chantilly editions share the imported country code with Longchamp.
    await connection.run(`
      UPDATE pc_keiba.jvd_se SET kakutei_chakujun='00' WHERE ijo_kubun_code IN ('4','5');
      INSERT INTO pc_keiba.jvd_ra SELECT * REPLACE ('2016' AS kaisai_nen, '1010' AS kaisai_tsukihi) FROM pc_keiba.jvd_ra WHERE kaisai_nen='2025';
      INSERT INTO pc_keiba.jvd_ra SELECT * REPLACE ('2017' AS kaisai_nen, '1010' AS kaisai_tsukihi) FROM pc_keiba.jvd_ra WHERE kaisai_nen='2025';
      INSERT INTO pc_keiba.jvd_se SELECT * REPLACE ('2016' AS kaisai_nen, '1010' AS kaisai_tsukihi) FROM pc_keiba.jvd_se WHERE kaisai_nen='2026';
      INSERT INTO pc_keiba.jvd_se SELECT * REPLACE ('2017' AS kaisai_nen, '1010' AS kaisai_tsukihi) FROM pc_keiba.jvd_se WHERE kaisai_nen='2026';
      INSERT INTO pc_keiba.oversea_runner_source_id SELECT * REPLACE ('2016' AS kaisai_nen, '1010' AS kaisai_tsukihi) FROM pc_keiba.oversea_runner_source_id WHERE kaisai_nen='2026';
      INSERT INTO pc_keiba.oversea_runner_source_id SELECT * REPLACE ('2017' AS kaisai_nen, '1010' AS kaisai_tsukihi) FROM pc_keiba.oversea_runner_source_id WHERE kaisai_nen='2026';
    `);
    const sameVenue = (
      await connection.runAndReadAll(
        buildConditionWeightClassStatsQuery({
          env: config,
          filters: { ...filters, includeVenue: true },
          kind: "carried",
        }),
      )
    ).getRowObjectsJson();
    expect(sameVenue.map((r) => [Number(r.starts), Number(r.win_count)])).toStrictEqual([[1, 0]]);
    const allVenues = (
      await connection.runAndReadAll(
        buildConditionWeightClassStatsQuery({
          env: config,
          filters,
          kind: "carried",
        }),
      )
    ).getRowObjectsJson();
    expect(allVenues.map((r) => [Number(r.starts), Number(r.win_count)])).toStrictEqual([[3, 2]]);
    const forcedVenue = (
      await connection.runAndReadAll(buildConditionFrameStatsQuery(config, filters))
    ).getRowObjectsJson();
    expect(forcedVenue.map((r) => [Number(r.count), Number(r.win_count)])).toStrictEqual([[1, 0]]);
    const sameVenueResults = (
      await connection.runAndReadAll(
        buildConditionFinishPositionStatsQuery(config, { ...filters, includeVenue: true }),
      )
    ).getRowObjectsJson();
    expect(sameVenueResults).toStrictEqual([]);
    const allVenueResults = (
      await connection.runAndReadAll(buildConditionFinishPositionStatsQuery(config, filters))
    ).getRowObjectsJson();
    expect(allVenueResults.map((r) => r.target_race_date)).toStrictEqual(["20171010", "20161010"]);
  } finally {
    connection.closeSync();
    instance.closeSync();
  }
});

it("does not apply the Chantilly exception to native or other overseas races", () => {
  expect(buildConditionFrameStatsQuery(config, jraFilters)).not.toMatch("NOT IN ('2016', '2017')");
  expect(
    buildConditionWeightClassStatsQuery({ env: config, filters: jraFilters, kind: "carried" }),
  ).not.toMatch("NOT IN ('2016', '2017')");
  expect(
    buildConditionFrameStatsQuery(config, {
      ...jraFilters,
      date: "20261004",
      keibajoCode: "A8",
      raceBango: "04",
    }),
  ).not.toMatch("NOT IN ('2016', '2017')");
});
