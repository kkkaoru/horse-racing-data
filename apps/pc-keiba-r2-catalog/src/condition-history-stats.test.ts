import { expect, it } from "vitest";

import {
  buildConditionFinishPositionStatsQuery,
  buildConditionFrameStatsQuery,
  buildConditionRaceTimeStatsQuery,
  buildConditionTargetRacesQuery,
  buildConditionWeightClassStatsQuery,
  normaliseTargetRaceRow,
  isBanEiKeibajo,
  normaliseConditionHistoryStatsPayload,
  normaliseFinishPositionRow,
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

it("builds finish-position and race-time aggregate SQL without detail arrays", () => {
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
  expect(finish).not.toMatch("THEN '芝'");
  expect(finish).not.toMatch("jsonb_agg");
  expect(time).toMatch("AS race_count");
  expect(time).toMatch("AS INT) = 1");
  expect(time).toMatch("approx_percentile_cont(race_time, 0.5)");
  expect(time).not.toMatch("jsonb_agg");
});

it("builds winner target-race list SQL without jsonb_agg", () => {
  const sql = buildConditionTargetRacesQuery(config, jraFilters);
  expect(sql).toMatch("AS target_race_date");
  expect(sql).toMatch("AS INT) = 1");
  expect(sql).toMatch("LIMIT 500");
  expect(sql).toMatch("FROM pc_keiba.jvd_se se");
  expect(sql).not.toMatch("jsonb_agg");
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
  ).toThrow("keibajoCode must contain two digits");
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

it("maps finish-position aggregates and always clears details", () => {
  expect(
    normaliseFinishPositionRow({
      average_odds: "12.34",
      average_popularity: 3.16,
      count: "4",
      finish_position: 1n,
      median_odds: "",
      median_popularity: null,
    }),
  ).toStrictEqual({
    averageOdds: 12.3,
    averagePopularity: 3.2,
    count: 4,
    details: [],
    finishPosition: 1,
    medianOdds: null,
    medianPopularity: null,
  });
});

it("maps race-time scalars and returns zeros when the query is empty", () => {
  expect(normaliseRaceTimeStats(undefined, [])).toStrictEqual({
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
          average_odds: 2,
          average_popularity: 1,
          count: 1,
          finish_position: 1,
          median_odds: 2,
          median_popularity: 1,
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
        details: [],
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
  expect(() =>
    normaliseFinishPositionRow({
      count: 1.5,
      finish_position: 1,
    }),
  ).toThrow("R2 SQL row is missing count");
  expect(() =>
    normaliseRaceTimeStats(
      {
        race_count: 2n ** 1024n,
      },
      [],
    ),
  ).toThrow("R2 SQL row is missing race_count");
  expect(() =>
    normaliseConditionHistoryStatsPayload({
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
      weightRows: [],
    }),
  ).toThrow("R2 SQL row is missing frame_number");
});
