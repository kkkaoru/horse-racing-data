// This file runs with bun.

import { expect, it } from "vitest";

import {
  buildFavoriteConditionRates,
  buildFinishHorsePoints,
  buildFinishOddsDistributionView,
  buildPayoutDistributionView,
  buildTukeyBox,
  CONDITION_FINISH_CHART_NOTE,
  CONDITION_PAYOUT_CHART_NOTE,
  DEFAULT_CONDITION_FINISH_CHART,
  DEFAULT_CONDITION_PAYOUT_CHART,
  FINISH_GROUP_LABELS,
  finishBoxForGroup,
  finishGroupLabel,
  finishGroupLabels,
  finishOddsLogTicks,
  finishOddsValues,
  finishTooltipContent,
  formatAnalysisOdds,
  formatAnalysisRate,
  formatAnalysisYen,
  formatChartRaceMeta,
  formatFavoriteConditionRates,
  hasFinishChartData,
  hasFinishOddsChartData,
  hasPayoutChartData,
  paddedLogDomain,
  payoutBeeJitter,
  payoutBoxForBetType,
  payoutCategoryLabels,
  payoutLogTicks,
  payoutTooltipContent,
  payoutYenValues,
  quantileSorted,
} from "./condition-analysis-charts";
import type { FinishPositionStatsRow, PayoutStatsRow, StatsDetail } from "./race-types";

const payoutRow = (overrides: Partial<PayoutStatsRow>): PayoutStatsRow => ({
  averagePayout: 800,
  betType: "単勝",
  count: 10,
  details: [],
  maxPayout: 2000,
  medianPayout: 500,
  minPayout: 200,
  ...overrides,
});

const finishRow = (overrides: Partial<FinishPositionStatsRow>): FinishPositionStatsRow => ({
  averageOdds: 8.2,
  averagePopularity: 4.1,
  count: 12,
  details: [],
  finishPosition: 1,
  medianOdds: 5.4,
  medianPopularity: 3,
  ...overrides,
});

const detail = (overrides: Partial<StatsDetail>): StatsDetail => ({
  date: "20260111",
  frameNumber: "1",
  horseName: "Alpha",
  horseNumber: "01",
  jockeyName: "Jockey A",
  keibajoCode: "05",
  popularity: "1",
  raceName: "一般",
  raceNumber: "01",
  raceTime: "1123",
  rank: "01",
  winOdds: "25",
  ...overrides,
});

it("defaults condition analysis views to the chart", () => {
  expect(DEFAULT_CONDITION_PAYOUT_CHART).toBe(true);
  expect(DEFAULT_CONDITION_FINISH_CHART).toBe(true);
  expect(CONDITION_PAYOUT_CHART_NOTE).toBe(
    "箱は四分位、ひげは外れ値を除いた範囲、点は各レースの払戻です。縦軸は対数なので、単勝から三連単まで分布の形と大穴を同じ図で比較できます。",
  );
  expect(CONDITION_FINISH_CHART_NOTE).toBe(
    "箱は着順グループごとの単勝オッズの四分位、ひげは外れ値を除いた範囲、点は各馬です。縦軸は対数なので本命と大穴の分布を同じ図で比べられます。",
  );
  expect(FINISH_GROUP_LABELS).toStrictEqual(["1着", "2着", "3着", "着外"]);
});

it("returns null quantiles for an empty series", () => {
  expect(quantileSorted([], 0.5)).toBe(null);
});

it("interpolates quantiles on a sorted series", () => {
  expect(quantileSorted([1, 2, 3, 4], 0)).toBe(1);
  expect(quantileSorted([1, 2, 3, 4], 0.5)).toBe(2.5);
  expect(quantileSorted([1, 2, 3, 4], 1)).toBe(4);
});

it("builds a Tukey box and flags a high outlier", () => {
  expect(buildTukeyBox([1, 2, 3], "単勝")).toBe(null);
  expect(buildTukeyBox([1, 2, 3, 4, 100], "単勝")).toStrictEqual({
    betType: "単勝",
    count: 5,
    median: 3,
    q1: 2,
    q3: 4,
    samples: [1, 2, 3, 4, 100],
    whiskerHigh: 4,
    whiskerLow: 1,
  });
});

it("builds payout box plots from race details and ranges from summaries", () => {
  expect(
    buildPayoutDistributionView([
      payoutRow({
        betType: "単勝",
        details: [
          { date: "20260101", keibajoCode: "05", payout: 200, raceName: "A", raceNumber: "01" },
          { date: "20260102", keibajoCode: "05", payout: 300, raceName: "B", raceNumber: "01" },
          { date: "20260103", keibajoCode: "05", payout: 400, raceName: "C", raceNumber: "01" },
          { date: "20260104", keibajoCode: "05", payout: 500, raceName: "D", raceNumber: "01" },
          { date: "20260105", keibajoCode: "05", payout: 8000, raceName: "E", raceNumber: "01" },
        ],
      }),
      payoutRow({ betType: "馬連" }),
      payoutRow({
        averagePayout: null,
        betType: "枠連",
        maxPayout: null,
        medianPayout: null,
        minPayout: null,
      }),
    ]),
  ).toStrictEqual({
    bees: [
      {
        betType: "単勝",
        date: "20260101",
        index: 0,
        isOutlier: false,
        raceName: "A",
        yen: 200,
      },
      {
        betType: "単勝",
        date: "20260102",
        index: 1,
        isOutlier: false,
        raceName: "B",
        yen: 300,
      },
      {
        betType: "単勝",
        date: "20260103",
        index: 2,
        isOutlier: false,
        raceName: "C",
        yen: 400,
      },
      {
        betType: "単勝",
        date: "20260104",
        index: 3,
        isOutlier: false,
        raceName: "D",
        yen: 500,
      },
      {
        betType: "単勝",
        date: "20260105",
        index: 4,
        isOutlier: true,
        raceName: "E",
        yen: 8000,
      },
    ],
    boxes: [
      {
        betType: "単勝",
        count: 5,
        median: 400,
        q1: 300,
        q3: 500,
        samples: [200, 300, 400, 500, 8000],
        whiskerHigh: 500,
        whiskerLow: 200,
      },
    ],
    ranges: [
      {
        average: 800,
        betType: "馬連",
        count: 10,
        max: 2000,
        median: 500,
        min: 200,
      },
    ],
  });
});

it("orders mixed box and range bet types for the log chart", () => {
  expect(
    payoutCategoryLabels(
      buildPayoutDistributionView([
        payoutRow({ betType: "馬連" }),
        payoutRow({
          betType: "単勝",
          details: [
            { date: "20260101", keibajoCode: "05", payout: 200, raceName: "A", raceNumber: "01" },
            { date: "20260102", keibajoCode: "05", payout: 300, raceName: "B", raceNumber: "01" },
            { date: "20260103", keibajoCode: "05", payout: 400, raceName: "C", raceNumber: "01" },
            { date: "20260104", keibajoCode: "05", payout: 500, raceName: "D", raceNumber: "01" },
          ],
        }),
        payoutRow({
          betType: "複勝",
          details: [
            { date: "20260101", keibajoCode: "05", payout: 110, raceName: "A", raceNumber: "01" },
            { date: "20260102", keibajoCode: "05", payout: 120, raceName: "B", raceNumber: "01" },
            { date: "20260103", keibajoCode: "05", payout: 130, raceName: "C", raceNumber: "01" },
            { date: "20260104", keibajoCode: "05", payout: 140, raceName: "D", raceNumber: "01" },
          ],
        }),
        payoutRow({ betType: "その他" }),
      ]),
    ),
  ).toStrictEqual(["単勝", "複勝", "馬連", "その他"]);
});

it("skips payout rows whose summary range is not positive", () => {
  expect(
    buildPayoutDistributionView([
      payoutRow({
        betType: "ワイド",
        maxPayout: 100,
        minPayout: 0,
      }),
    ]),
  ).toStrictEqual({ bees: [], boxes: [], ranges: [] });
});

it("collects yen values including a missing average", () => {
  expect(
    payoutYenValues({
      bees: [],
      boxes: [],
      ranges: [
        {
          average: null,
          betType: "馬連",
          count: 4,
          max: 2000,
          median: 500,
          min: 200,
        },
      ],
    }),
  ).toStrictEqual([200, 500, 2000]);
  expect(
    payoutYenValues({
      bees: [],
      boxes: [],
      ranges: [
        {
          average: 800,
          betType: "馬連",
          count: 4,
          max: 2000,
          median: 500,
          min: 200,
        },
      ],
    }),
  ).toStrictEqual([200, 500, 2000, 800]);
});

it("reports whether a payout view can be drawn", () => {
  expect(hasPayoutChartData({ bees: [], boxes: [], ranges: [] })).toBe(false);
  expect(
    hasPayoutChartData(
      buildPayoutDistributionView([
        payoutRow({
          details: [
            { date: "20260101", keibajoCode: "05", payout: 500, raceName: "A", raceNumber: "01" },
            { date: "20260102", keibajoCode: "05", payout: 500, raceName: "B", raceNumber: "01" },
            { date: "20260103", keibajoCode: "05", payout: 500, raceName: "C", raceNumber: "01" },
            { date: "20260104", keibajoCode: "05", payout: 500, raceName: "D", raceNumber: "01" },
          ],
        }),
      ]),
    ),
  ).toBe(true);
});

it("pads a log domain when every payout is the same", () => {
  expect(paddedLogDomain(500, 500)).toStrictEqual([416.6666666666667, 600]);
  expect(paddedLogDomain(200, 8000)).toStrictEqual([200, 8000]);
});

it("uses listed log ticks inside the domain and domain ends when none fit", () => {
  expect(payoutLogTicks([200, 8000])).toStrictEqual([300, 1000, 3000]);
  expect(payoutLogTicks([416.6666666666667, 600])).toStrictEqual([416.6666666666667, 600]);
});

it("returns a deterministic beeswarm jitter offset", () => {
  expect(payoutBeeJitter(0)).toBe(-10.714);
});

it("finds the Tukey box for a bet type", () => {
  expect(payoutBoxForBetType([], "単勝")).toBe(null);
  expect(
    payoutBoxForBetType(
      [
        {
          betType: "単勝",
          count: 4,
          median: 400,
          q1: 300,
          q3: 500,
          samples: [200, 300, 400, 500],
          whiskerHigh: 500,
          whiskerLow: 200,
        },
      ],
      "単勝",
    )?.betType,
  ).toBe("単勝");
});

it("builds finish horse points, groups 着外, and drops rows without finish or odds", () => {
  expect(finishGroupLabel(1)).toBe("1着");
  expect(finishGroupLabel(2)).toBe("2着");
  expect(finishGroupLabel(3)).toBe("3着");
  expect(finishGroupLabel(4)).toBe("着外");
  expect(finishGroupLabel(8)).toBe("着外");
  expect(
    buildFinishHorsePoints([
      finishRow({
        details: [
          detail({ horseName: "Win", popularity: "1", rank: "01", winOdds: "25" }),
          detail({ horseName: "Place", popularity: "2", rank: "02", winOdds: "40" }),
          detail({ horseName: "Show", popularity: "4", rank: "03", winOdds: "80" }),
          detail({ horseName: "Rest", popularity: "10", rank: "08", winOdds: "210" }),
          detail({ horseName: "NoPop", popularity: "0", rank: "01", winOdds: "25" }),
          detail({ horseName: "NoFinish", popularity: "1", rank: "00", winOdds: "25" }),
          detail({ horseName: "NoOdds", popularity: "1", rank: "01", winOdds: "0" }),
        ],
      }),
    ]),
  ).toStrictEqual([
    {
      date: "20260111",
      finishGroup: "1着",
      finishPosition: 1,
      horseName: "Win",
      odds: 2.5,
      popularity: 1,
      raceName: "一般",
    },
    {
      date: "20260111",
      finishGroup: "2着",
      finishPosition: 2,
      horseName: "Place",
      odds: 4,
      popularity: 2,
      raceName: "一般",
    },
    {
      date: "20260111",
      finishGroup: "3着",
      finishPosition: 3,
      horseName: "Show",
      odds: 8,
      popularity: 4,
      raceName: "一般",
    },
    {
      date: "20260111",
      finishGroup: "着外",
      finishPosition: 8,
      horseName: "Rest",
      odds: 21,
      popularity: 10,
      raceName: "一般",
    },
    {
      date: "20260111",
      finishGroup: "1着",
      finishPosition: 1,
      horseName: "NoPop",
      odds: 2.5,
      popularity: 0,
      raceName: "一般",
    },
  ]);
});

it("keeps already-decimal win odds instead of dividing them by 10", () => {
  expect(
    buildFinishHorsePoints([
      finishRow({
        details: [detail({ horseName: "Decimal", popularity: "3", rank: "01", winOdds: "2.5" })],
      }),
    ]),
  ).toStrictEqual([
    {
      date: "20260111",
      finishGroup: "1着",
      finishPosition: 1,
      horseName: "Decimal",
      odds: 2.5,
      popularity: 3,
      raceName: "一般",
    },
  ]);
});

it("builds finish points from catalog aggregate odds when details are empty", () => {
  expect(
    buildFinishHorsePoints([
      finishRow({
        averageOdds: 3.2,
        averagePopularity: 2.5,
        details: [],
        finishPosition: 1,
        medianOdds: 3,
        medianPopularity: 2,
      }),
      finishRow({
        averageOdds: 8.1,
        averagePopularity: 6.4,
        details: [],
        finishPosition: 4,
        medianOdds: 7.5,
        medianPopularity: 6,
      }),
      finishRow({
        averageOdds: null,
        averagePopularity: null,
        details: [],
        finishPosition: 2,
        medianOdds: null,
        medianPopularity: null,
      }),
      finishRow({
        averageOdds: 5,
        averagePopularity: 4,
        details: [],
        finishPosition: 0,
        medianOdds: 4.5,
        medianPopularity: 3,
      }),
    ]),
  ).toStrictEqual([
    {
      date: "",
      finishGroup: "1着",
      finishPosition: 1,
      horseName: "1着",
      odds: 3,
      popularity: 2,
      raceName: "",
    },
    {
      date: "",
      finishGroup: "着外",
      finishPosition: 4,
      horseName: "着外",
      odds: 7.5,
      popularity: 6,
      raceName: "",
    },
  ]);
});

it("prefers horse details over catalog aggregates when both exist", () => {
  expect(
    buildFinishHorsePoints([
      finishRow({
        averageOdds: 9.9,
        averagePopularity: 9,
        details: [detail({ horseName: "Win", popularity: "1", rank: "01", winOdds: "25" })],
        medianOdds: 9.8,
        medianPopularity: 8,
      }),
    ]),
  ).toStrictEqual([
    {
      date: "20260111",
      finishGroup: "1着",
      finishPosition: 1,
      horseName: "Win",
      odds: 2.5,
      popularity: 1,
      raceName: "一般",
    },
  ]);
});

it("uses average odds and popularity when catalog medians are missing", () => {
  expect(
    buildFinishHorsePoints([
      finishRow({
        averageOdds: 4.8,
        averagePopularity: 3.1,
        details: [],
        finishPosition: 2,
        medianOdds: null,
        medianPopularity: null,
      }),
    ]),
  ).toStrictEqual([
    {
      date: "",
      finishGroup: "2着",
      finishPosition: 2,
      horseName: "2着",
      odds: 4.8,
      popularity: 3.1,
      raceName: "",
    },
  ]);
});

it("builds Tukey boxes and bees by finish group and flags an odds outlier", () => {
  expect(
    buildFinishOddsDistributionView([
      {
        date: "20260111",
        finishGroup: "1着",
        finishPosition: 1,
        horseName: "A",
        odds: 1.5,
        popularity: 1,
        raceName: "一般",
      },
      {
        date: "20260111",
        finishGroup: "1着",
        finishPosition: 1,
        horseName: "B",
        odds: 2,
        popularity: 2,
        raceName: "一般",
      },
      {
        date: "20260111",
        finishGroup: "1着",
        finishPosition: 1,
        horseName: "C",
        odds: 2.5,
        popularity: 3,
        raceName: "一般",
      },
      {
        date: "20260111",
        finishGroup: "1着",
        finishPosition: 1,
        horseName: "D",
        odds: 3,
        popularity: 4,
        raceName: "一般",
      },
      {
        date: "20260112",
        finishGroup: "1着",
        finishPosition: 1,
        horseName: "Hole",
        odds: 80,
        popularity: 12,
        raceName: "特別",
      },
      {
        date: "20260111",
        finishGroup: "着外",
        finishPosition: 8,
        horseName: "Rest",
        odds: 21,
        popularity: 10,
        raceName: "一般",
      },
    ]),
  ).toStrictEqual({
    bees: [
      {
        date: "20260111",
        finishGroup: "1着",
        finishPosition: 1,
        horseName: "A",
        index: 0,
        isOutlier: false,
        odds: 1.5,
        popularity: 1,
        raceName: "一般",
      },
      {
        date: "20260111",
        finishGroup: "1着",
        finishPosition: 1,
        horseName: "B",
        index: 1,
        isOutlier: false,
        odds: 2,
        popularity: 2,
        raceName: "一般",
      },
      {
        date: "20260111",
        finishGroup: "1着",
        finishPosition: 1,
        horseName: "C",
        index: 2,
        isOutlier: false,
        odds: 2.5,
        popularity: 3,
        raceName: "一般",
      },
      {
        date: "20260111",
        finishGroup: "1着",
        finishPosition: 1,
        horseName: "D",
        index: 3,
        isOutlier: false,
        odds: 3,
        popularity: 4,
        raceName: "一般",
      },
      {
        date: "20260112",
        finishGroup: "1着",
        finishPosition: 1,
        horseName: "Hole",
        index: 4,
        isOutlier: true,
        odds: 80,
        popularity: 12,
        raceName: "特別",
      },
      {
        date: "20260111",
        finishGroup: "着外",
        finishPosition: 8,
        horseName: "Rest",
        index: 0,
        isOutlier: false,
        odds: 21,
        popularity: 10,
        raceName: "一般",
      },
    ],
    boxes: [
      {
        betType: "1着",
        count: 5,
        median: 2.5,
        q1: 2,
        q3: 3,
        samples: [1.5, 2, 2.5, 3, 80],
        whiskerHigh: 3,
        whiskerLow: 1.5,
      },
    ],
  });
});

it("keeps a single finish group without a Tukey box when samples are few", () => {
  expect(
    buildFinishOddsDistributionView([
      {
        date: "20260111",
        finishGroup: "1着",
        finishPosition: 1,
        horseName: "Win",
        odds: 2.5,
        popularity: 1,
        raceName: "一般",
      },
    ]),
  ).toStrictEqual({
    bees: [
      {
        date: "20260111",
        finishGroup: "1着",
        finishPosition: 1,
        horseName: "Win",
        index: 0,
        isOutlier: false,
        odds: 2.5,
        popularity: 1,
        raceName: "一般",
      },
    ],
    boxes: [],
  });
});

it("classifies finish odds chart domains, labels, and log ticks", () => {
  expect(hasFinishChartData([])).toBe(false);
  expect(hasFinishOddsChartData({ bees: [], boxes: [] })).toBe(false);
  expect(finishGroupLabels({ bees: [], boxes: [] })).toStrictEqual([]);
  expect(
    hasFinishChartData([
      {
        date: "20260111",
        finishGroup: "1着",
        finishPosition: 1,
        horseName: "Win",
        odds: 2.5,
        popularity: 1,
        raceName: "一般",
      },
    ]),
  ).toBe(true);
  expect(
    finishGroupLabels(
      buildFinishOddsDistributionView([
        {
          date: "20260111",
          finishGroup: "1着",
          finishPosition: 1,
          horseName: "Win",
          odds: 2.5,
          popularity: 1,
          raceName: "一般",
        },
        {
          date: "20260111",
          finishGroup: "着外",
          finishPosition: 8,
          horseName: "Rest",
          odds: 21,
          popularity: 10,
          raceName: "一般",
        },
      ]),
    ),
  ).toStrictEqual(["1着", "着外"]);
  expect(
    finishOddsValues({
      bees: [
        {
          date: "20260111",
          finishGroup: "1着",
          finishPosition: 1,
          horseName: "Win",
          index: 0,
          isOutlier: false,
          odds: 2.5,
          popularity: 1,
          raceName: "一般",
        },
      ],
      boxes: [],
    }),
  ).toStrictEqual([2.5]);
  expect(
    hasFinishOddsChartData(
      buildFinishOddsDistributionView([
        {
          date: "20260111",
          finishGroup: "1着",
          finishPosition: 1,
          horseName: "Win",
          odds: 2.5,
          popularity: 1,
          raceName: "一般",
        },
      ]),
    ),
  ).toBe(true);
  expect(
    hasFinishOddsChartData(
      buildFinishOddsDistributionView([
        {
          date: "20260111",
          finishGroup: "着外",
          finishPosition: 8,
          horseName: "Rest",
          odds: 21,
          popularity: 10,
          raceName: "一般",
        },
      ]),
    ),
  ).toBe(true);
  expect(
    hasFinishOddsChartData({
      bees: [
        {
          date: "20260111",
          finishGroup: "1着",
          finishPosition: 1,
          horseName: "Zero",
          index: 0,
          isOutlier: false,
          odds: 0,
          popularity: 1,
          raceName: "一般",
        },
      ],
      boxes: [],
    }),
  ).toBe(false);
  expect(finishOddsLogTicks([2, 80])).toStrictEqual([2, 3, 5, 10, 20, 30, 50]);
  expect(finishOddsLogTicks([2.083333333333333, 3])).toStrictEqual([3]);
  expect(finishOddsLogTicks([2.2, 2.4])).toStrictEqual([2.2, 2.4]);
  expect(paddedLogDomain(2.5, 2.5)).toStrictEqual([2.0833333333333335, 3]);
  expect(finishBoxForGroup([], "1着")).toBe(null);
  expect(
    finishBoxForGroup(
      [
        {
          betType: "1着",
          count: 4,
          median: 2.5,
          q1: 2,
          q3: 3,
          samples: [1.5, 2, 2.5, 3],
          whiskerHigh: 3,
          whiskerLow: 1.5,
        },
      ],
      "1着",
    )?.betType,
  ).toBe("1着");
  expect(formatAnalysisOdds(2)).toBe("2");
  expect(formatAnalysisOdds(2.5)).toBe("2.5");
});

it("returns empty collections when there is nothing to plot", () => {
  expect(buildFinishHorsePoints([])).toStrictEqual([]);
  expect(buildFinishOddsDistributionView([])).toStrictEqual({ bees: [], boxes: [] });
  expect(buildPayoutDistributionView([])).toStrictEqual({ bees: [], boxes: [], ranges: [] });
});

it("returns null favorite rates when no 1番人気 details exist", () => {
  expect(
    buildFavoriteConditionRates([
      finishRow({
        details: [
          detail({ popularity: "2", rank: "01" }),
          detail({ popularity: "1", rank: "00" }),
          detail({ popularity: "", rank: "01" }),
          detail({ popularity: "1", rank: "-1" }),
        ],
      }),
    ]),
  ).toBe(null);
});

it("computes 1番人気 win, quinella, and show rates from finish details", () => {
  expect(
    buildFavoriteConditionRates([
      finishRow({
        details: [
          detail({ popularity: "1", rank: "01" }),
          detail({ popularity: "1", rank: "02" }),
          detail({ popularity: "1", rank: "03" }),
          detail({ popularity: "1", rank: "04" }),
          detail({ popularity: "3", rank: "01" }),
        ],
      }),
    ]),
  ).toStrictEqual({
    quinellaRate: 50,
    showRate: 75,
    starts: 4,
    winRate: 25,
  });
});

it("formats yen in 円 below 1万 and 万 at and above 1万", () => {
  expect(formatAnalysisYen(350)).toBe("350円");
  expect(formatAnalysisYen(10000)).toBe("1.0万");
  expect(formatAnalysisYen(12500)).toBe("1.3万");
  expect(formatAnalysisYen(120000)).toBe("12万");
});

it("formats a percent with one decimal", () => {
  expect(formatAnalysisRate(12.5)).toBe("12.5%");
  expect(formatAnalysisRate(0)).toBe("0.0%");
});

it("formats the 1番人気 summary in Japanese", () => {
  expect(
    formatFavoriteConditionRates({
      quinellaRate: 40,
      showRate: 55.5,
      starts: 12,
      winRate: 16.7,
    }),
  ).toBe("この条件の1番人気は勝率16.7% / 連対率40.0% / 複勝率55.5%（12頭）");
});

it("formats race meta for tooltips", () => {
  expect(formatChartRaceMeta({ date: "", raceName: "" })).toBe(null);
  expect(formatChartRaceMeta({ date: "", raceName: "一般" })).toBe("一般");
  expect(formatChartRaceMeta({ date: "2026-01-11", raceName: "" })).toBe("2026-01-11");
  expect(formatChartRaceMeta({ date: "20260111", raceName: "一般" })).toBe("一般 / 2026/01/11");
});

it("builds a payout tooltip with quartile context and an outlier flag", () => {
  expect(
    payoutTooltipContent({
      bee: {
        betType: "単勝",
        date: "20260105",
        index: 4,
        isOutlier: true,
        raceName: "E",
        yen: 8000,
      },
      box: {
        betType: "単勝",
        count: 5,
        median: 400,
        q1: 300,
        q3: 500,
        samples: [200, 300, 400, 500, 8000],
        whiskerHigh: 500,
        whiskerLow: 200,
      },
    }),
  ).toStrictEqual({
    lines: ["8,000円", "外れ値", "Q1 300円 / 中央値 400円 / Q3 500円"],
    meta: "E / 2026/01/05",
    title: "単勝",
  });
});

it("builds a payout tooltip without a box and without race meta", () => {
  expect(
    payoutTooltipContent({
      bee: {
        betType: "馬連",
        date: "",
        index: 0,
        isOutlier: false,
        raceName: "",
        yen: 500,
      },
      box: null,
    }),
  ).toStrictEqual({
    lines: ["500円", "箱ひげ内"],
    meta: null,
    title: "馬連",
  });
});

it("builds a finish tooltip with horse, market, fence, and quartile detail", () => {
  expect(
    finishTooltipContent({
      bee: {
        date: "20260112",
        finishGroup: "1着",
        finishPosition: 1,
        horseName: "Hole",
        index: 4,
        isOutlier: true,
        odds: 80,
        popularity: 12,
        raceName: "特別",
      },
      box: {
        betType: "1着",
        count: 5,
        median: 2.5,
        q1: 2,
        q3: 3,
        samples: [1.5, 2, 2.5, 3, 80],
        whiskerHigh: 3,
        whiskerLow: 1.5,
      },
    }),
  ).toStrictEqual({
    lines: ["1着", "人気 12", "オッズ 80", "外れ値", "Q1 2 / 中央値 2.5 / Q3 3"],
    meta: "特別 / 2026/01/12",
    title: "Hole",
  });
});

it("builds a finish tooltip without a box", () => {
  expect(
    finishTooltipContent({
      bee: {
        date: "",
        finishGroup: "1着",
        finishPosition: 1,
        horseName: "Win Horse",
        index: 0,
        isOutlier: false,
        odds: 2.5,
        popularity: 1,
        raceName: "",
      },
      box: null,
    }),
  ).toStrictEqual({
    lines: ["1着", "人気 1", "オッズ 2.5", "箱ひげ内"],
    meta: null,
    title: "Win Horse",
  });
});
