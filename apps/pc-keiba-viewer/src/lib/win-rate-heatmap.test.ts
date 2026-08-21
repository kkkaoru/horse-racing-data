// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import type {
  BloodlineStatsRow,
  FrameStatsRow,
  HorseRaceResult,
  Runner,
  SimilarRaceStatsRow,
  StatsDetail,
} from "./race-types";
import {
  buildWinRateHeatmapColorScaleGradient,
  buildWinRateHeatmapRows,
  DEFAULT_WIN_RATE_HEATMAP_VIEW_MODE,
  formatWinRateHeatmapColorScaleAriaLabel,
  formatWinRateHeatmapColorScaleCaption,
  formatWinRateHeatmapColorScaleTick,
  formatWinRateHeatmapStarts,
  formatWinRateHeatmapTooltipStarts,
  formatWinRateHeatmapValue,
  getVisibleWinRateHeatmapColumns,
  shouldShowWinRateHeatmapCarriedWeightColumn,
  getVisibleWinRateHeatmapRateMetrics,
  getWinRateHeatmapColorScaleTracks,
  getWinRateHeatmapTooltipName,
  shouldShowWinRateHeatmapWeightColumn,
  WIN_RATE_HEATMAP_COLOR_SCALE_MAX_RATE,
  WIN_RATE_HEATMAP_COLOR_SCALE_TICKS,
  WIN_RATE_HEATMAP_COLUMNS,
  WIN_RATE_HEATMAP_COMBINED_HUE,
  WIN_RATE_HEATMAP_RATE_METRICS,
  WIN_RATE_HEATMAP_VIEW_MODES,
  winRateHeatmapBackground,
  winRateHeatmapEntityColSpan,
} from "./win-rate-heatmap";

const runnerOne: Runner = {
  banushimei: "Owner A",
  barei: "4",
  bamei: "Alpha",
  bataiju: null,
  chokyoshimeiRyakusho: "Trainer A",
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  damSireName: null,
  futanJuryo: "570",
  kakuteiChakujun: "00",
  kettoTorokuBango: "2020100001",
  kishumeiRyakusho: "Jockey A",
  kohan3f: null,
  seibetsuCode: "1",
  sireName: null,
  sireSireName: null,
  sohaTime: null,
  tanshoNinkijun: "00",
  tanshoOdds: "0000",
  timeSa: null,
  umaban: "01",
  wakuban: "1",
  zogenFugo: null,
  zogenSa: null,
};

const runnerTwo: Runner = {
  banushimei: "Owner B",
  barei: "5",
  bamei: "Beta",
  bataiju: null,
  chokyoshimeiRyakusho: "Trainer B",
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  damSireName: null,
  futanJuryo: "560",
  kakuteiChakujun: "00",
  kettoTorokuBango: "2020100002",
  kishumeiRyakusho: "Jockey B",
  kohan3f: null,
  seibetsuCode: "2",
  sireName: null,
  sireSireName: null,
  sohaTime: null,
  tanshoNinkijun: "00",
  tanshoOdds: "0000",
  timeSa: null,
  umaban: "02",
  wakuban: "2",
  zogenFugo: null,
  zogenSa: null,
};

const similarJockey: SimilarRaceStatsRow = {
  category: "jockey",
  currentHorseNumbers: "1",
  details: [],
  horseCount: 40,
  name: "Jockey A",
  quinellaCount: 20,
  quinellaRate: 25,
  showCount: 30,
  showRate: 37.5,
  starts: 80,
  winCount: 16,
  winRate: 20,
};

const similarTrainer: SimilarRaceStatsRow = {
  category: "trainer",
  currentHorseNumbers: "1, 2",
  details: [],
  horseCount: 12,
  name: "Shared Trainer",
  quinellaCount: 8,
  quinellaRate: 16,
  showCount: 10,
  showRate: 20,
  starts: 50,
  winCount: 5,
  winRate: 10,
};

const bloodlineSire: BloodlineStatsRow = {
  category: "sire",
  currentHorseNumbers: "2",
  details: [],
  horseCount: 30,
  name: "Sire Beta",
  quinellaCount: 40,
  quinellaRate: 20,
  showCount: 60,
  showRate: 30,
  starts: 200,
  winCount: 24,
  winRate: 12,
};

const bloodlineDamSire: BloodlineStatsRow = {
  category: "damSire",
  currentHorseNumbers: "2",
  details: [],
  horseCount: 18,
  name: "Dam Sire Beta",
  quinellaCount: 22,
  quinellaRate: 11,
  showCount: 40,
  showRate: 20,
  starts: 200,
  winCount: 8,
  winRate: 4,
};

const bloodlineSireSire: BloodlineStatsRow = {
  category: "sireSire",
  currentHorseNumbers: "2",
  details: [],
  horseCount: 50,
  name: "Sire Sire Beta",
  quinellaCount: 80,
  quinellaRate: 16,
  showCount: 120,
  showRate: 24,
  starts: 500,
  winCount: 40,
  winRate: 8,
};

const frameOne: FrameStatsRow = {
  averageFinish: 3.2,
  averagePopularity: 4.1,
  count: 40,
  details: [],
  frameNumber: "1",
  medianFinish: 2.5,
  medianPopularity: 3,
  quinellaCount: 12,
  quinellaRate: 30,
  runnerCount: 16,
  score: 0.8,
  showCount: 18,
  showRate: 45,
  winCount: 6,
  winRate: 15,
};

const frameWinDetail: StatsDetail = {
  date: "20250101",
  frameNumber: "1",
  horseName: "Past A",
  horseNumber: "01",
  jockeyName: "Jockey A",
  keibajoCode: "05",
  popularity: "1",
  raceName: "一般",
  raceNumber: "01",
  raceTime: "1200",
  rank: "01",
  winOdds: "30",
};

const frameSecondDetail: StatsDetail = {
  date: "20250201",
  frameNumber: "1",
  horseName: "Past B",
  horseNumber: "02",
  jockeyName: "Jockey B",
  keibajoCode: "05",
  popularity: "2",
  raceName: "一般",
  raceNumber: "02",
  raceTime: "1210",
  rank: "02",
  winOdds: "50",
};

const frameThirdDetail: StatsDetail = {
  date: "20250301",
  frameNumber: "1",
  horseName: "Past C",
  horseNumber: "03",
  jockeyName: "Jockey C",
  keibajoCode: "05",
  popularity: "3",
  raceName: "一般",
  raceNumber: "03",
  raceTime: "1220",
  rank: "03",
  winOdds: "80",
};

const frameFifthDetail: StatsDetail = {
  date: "20250401",
  frameNumber: "1",
  horseName: "Past D",
  horseNumber: "04",
  jockeyName: "Jockey D",
  keibajoCode: "05",
  popularity: "4",
  raceName: "一般",
  raceNumber: "04",
  raceTime: "1230",
  rank: "05",
  winOdds: "120",
};

const horseWin: HorseRaceResult = {
  babajotaiCodeDirt: null,
  babajotaiCodeShiba: null,
  bamei: "Alpha",
  banushimei: "Owner A",
  barei: "3",
  bataiju: null,
  chokyoshimeiRyakusho: "Trainer A",
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  currentBarei: "4",
  currentJockey: "Jockey A",
  currentSeibetsuCode: "1",
  currentUmaban: "01",
  futanJuryo: "570",
  gradeCode: null,
  hassoJikoku: "1200",
  juryoShubetsuCode: null,
  kaisaiNen: "2025",
  kaisaiTsukihi: "0112",
  kakuteiChakujun: "01",
  keibajoCode: "05",
  kettoTorokuBango: "2020100001",
  kishumeiRyakusho: "Jockey A",
  kohan3f: null,
  kyori: "1600",
  kyosoJokenCode: null,
  kyosoJokenMeisho: null,
  kyosoKigoCode: null,
  kyosomeiFukudai: null,
  kyosomeiHondai: "Past A",
  kyosomeiKakkonai: null,
  kyosoShubetsuCode: null,
  raceBango: "01",
  seibetsuCode: "1",
  sohaTime: null,
  tanshoNinkijun: "1",
  tanshoOdds: "12",
  tenkoCode: null,
  timeSa: null,
  trackCode: "10",
  umaban: "03",
  wakuban: "3",
  zogenFugo: null,
  zogenSa: null,
};

const horseSecond: HorseRaceResult = {
  babajotaiCodeDirt: null,
  babajotaiCodeShiba: null,
  bamei: "Alpha",
  banushimei: "Owner A",
  barei: "3",
  bataiju: null,
  chokyoshimeiRyakusho: "Trainer A",
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  currentBarei: "4",
  currentJockey: "Jockey A",
  currentSeibetsuCode: "1",
  currentUmaban: "01",
  futanJuryo: "570",
  gradeCode: null,
  hassoJikoku: "1300",
  juryoShubetsuCode: null,
  kaisaiNen: "2025",
  kaisaiTsukihi: "0202",
  kakuteiChakujun: "02",
  keibajoCode: "06",
  kettoTorokuBango: "2020100001",
  kishumeiRyakusho: "Jockey A",
  kohan3f: null,
  kyori: "1800",
  kyosoJokenCode: null,
  kyosoJokenMeisho: null,
  kyosoKigoCode: null,
  kyosomeiFukudai: null,
  kyosomeiHondai: "Past B",
  kyosomeiKakkonai: null,
  kyosoShubetsuCode: null,
  raceBango: "08",
  seibetsuCode: "1",
  sohaTime: null,
  tanshoNinkijun: "3",
  tanshoOdds: "45",
  tenkoCode: null,
  timeSa: null,
  trackCode: "10",
  umaban: "05",
  wakuban: "5",
  zogenFugo: null,
  zogenSa: null,
};

it("exports heatmap columns for frame, weight, carried weight, horse, jockey, trainer, and bloodline", () => {
  expect(WIN_RATE_HEATMAP_COLUMNS).toStrictEqual([
    { key: "frame", label: "枠" },
    { key: "weight", label: "馬体重" },
    { key: "carriedWeight", label: "斤量" },
    { key: "horse", label: "馬" },
    { key: "jockey", label: "騎手" },
    { key: "trainer", label: "調教師" },
    { key: "sire", label: "父" },
    { key: "damSire", label: "母父" },
    { key: "sireSire", label: "父父" },
  ]);
  expect(
    getVisibleWinRateHeatmapColumns({ showCarriedWeight: true, showWeight: true }),
  ).toStrictEqual([
    { key: "frame", label: "枠" },
    { key: "weight", label: "馬体重" },
    { key: "carriedWeight", label: "斤量" },
    { key: "horse", label: "馬" },
    { key: "jockey", label: "騎手" },
    { key: "trainer", label: "調教師" },
    { key: "sire", label: "父" },
    { key: "damSire", label: "母父" },
    { key: "sireSire", label: "父父" },
  ]);
  expect(
    getVisibleWinRateHeatmapColumns({ showCarriedWeight: false, showWeight: false }),
  ).toStrictEqual([
    { key: "frame", label: "枠" },
    { key: "horse", label: "馬" },
    { key: "jockey", label: "騎手" },
    { key: "trainer", label: "調教師" },
    { key: "sire", label: "父" },
    { key: "damSire", label: "母父" },
    { key: "sireSire", label: "父父" },
  ]);
});

it("exports win, quinella, and show rate metrics", () => {
  expect(WIN_RATE_HEATMAP_COMBINED_HUE).toBe(272);
  expect(WIN_RATE_HEATMAP_RATE_METRICS).toStrictEqual([
    { countKey: "winCount", hue: 8, key: "winRate", label: "勝率", shortLabel: "勝" },
    { countKey: "quinellaCount", hue: 36, key: "quinellaRate", label: "連対率", shortLabel: "連" },
    { countKey: "showCount", hue: 196, key: "showRate", label: "複勝率", shortLabel: "複" },
  ]);
});

it("returns no heatmap rows when there are no runners", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      bloodlineRows: [bloodlineSire],
      frameStats: [frameOne],
      horseResults: [horseWin],
      runners: [],
      similarRows: [similarJockey],
    }),
  ).toStrictEqual([]);
});

it("maps horse, jockey, trainer, and bloodline rates onto each horse", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      bloodlineRows: [bloodlineSire, bloodlineDamSire, bloodlineSireSire],
      frameStats: [frameOne],
      horseResults: [horseWin, horseSecond],
      runners: [runnerTwo, runnerOne],
      similarRows: [similarJockey, similarTrainer],
    }),
  ).toStrictEqual([
    {
      cells: {
        damSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        frame: {
          name: "枠1",
          quinellaCount: 12,
          quinellaRate: 30,
          showCount: 18,
          showRate: 45,
          starts: 40,
          winCount: 6,
          winRate: 15,
        },
        horse: {
          name: "Alpha",
          quinellaCount: 2,
          quinellaRate: 100,
          showCount: 2,
          showRate: 100,
          starts: 2,
          winCount: 1,
          winRate: 50,
        },
        jockey: {
          name: "Jockey A",
          quinellaCount: 20,
          quinellaRate: 25,
          showCount: 30,
          showRate: 37.5,
          starts: 80,
          winCount: 16,
          winRate: 20,
        },
        sire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        trainer: {
          name: "Shared Trainer",
          quinellaCount: 8,
          quinellaRate: 16,
          showCount: 10,
          showRate: 20,
          starts: 50,
          winCount: 5,
          winRate: 10,
        },
        carriedWeight: {
          name: "55.5kg以上57kg以下",
          quinellaCount: 2,
          quinellaRate: 100,
          showCount: 2,
          showRate: 100,
          starts: 2,
          winCount: 1,
          winRate: 50,
        },
        weight: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
      },
      frameNumber: "1",
      horseName: "Alpha",
      horseNumber: "1",
    },
    {
      cells: {
        damSire: {
          name: "Dam Sire Beta",
          quinellaCount: 22,
          quinellaRate: 11,
          showCount: 40,
          showRate: 20,
          starts: 200,
          winCount: 8,
          winRate: 4,
        },
        frame: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        horse: {
          name: "Beta",
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        jockey: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sire: {
          name: "Sire Beta",
          quinellaCount: 40,
          quinellaRate: 20,
          showCount: 60,
          showRate: 30,
          starts: 200,
          winCount: 24,
          winRate: 12,
        },
        sireSire: {
          name: "Sire Sire Beta",
          quinellaCount: 80,
          quinellaRate: 16,
          showCount: 120,
          showRate: 24,
          starts: 500,
          winCount: 40,
          winRate: 8,
        },
        trainer: {
          name: "Shared Trainer",
          quinellaCount: 8,
          quinellaRate: 16,
          showCount: 10,
          showRate: 20,
          starts: 50,
          winCount: 5,
          winRate: 10,
        },
        carriedWeight: {
          name: "55.5kg以上57kg以下",
          quinellaCount: 2,
          quinellaRate: 100,
          showCount: 2,
          showRate: 100,
          starts: 2,
          winCount: 1,
          winRate: 50,
        },
        weight: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
      },
      frameNumber: "2",
      horseName: "Beta",
      horseNumber: "2",
    },
  ]);
});

it("drops runners without a displayable horse number", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      bloodlineRows: [],
      frameStats: [frameOne],
      horseResults: [horseWin],
      runners: [{ ...runnerOne, umaban: "00" }],
      similarRows: [similarJockey],
    }),
  ).toStrictEqual([]);
});

it("looks up frame rates by wakuban when the stored frame number is zero-padded", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      bloodlineRows: [],
      frameStats: [{ ...frameOne, frameNumber: "01" }],
      horseResults: [],
      runners: [runnerOne],
      similarRows: [],
    }),
  ).toStrictEqual([
    {
      cells: {
        damSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        frame: {
          name: "枠1",
          quinellaCount: 12,
          quinellaRate: 30,
          showCount: 18,
          showRate: 45,
          starts: 40,
          winCount: 6,
          winRate: 15,
        },
        horse: {
          name: "Alpha",
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        jockey: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        trainer: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        carriedWeight: {
          name: "55.5kg以上57kg以下",
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        weight: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
      },
      frameNumber: "1",
      horseName: "Alpha",
      horseNumber: "1",
    },
  ]);
});

it("treats non-finite frame rates as missing heatmap values", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      bloodlineRows: [],
      frameStats: [
        {
          ...frameOne,
          count: Number.NaN,
          quinellaCount: Number.NaN,
          quinellaRate: Number.NaN,
          showCount: Number.NaN,
          showRate: Number.NaN,
          winCount: Number.NaN,
          winRate: Number.NaN,
        },
      ],
      horseResults: [],
      runners: [runnerOne],
      similarRows: [],
    }),
  ).toStrictEqual([
    {
      cells: {
        damSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        frame: {
          name: "枠1",
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        horse: {
          name: "Alpha",
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        jockey: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        trainer: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        carriedWeight: {
          name: "55.5kg以上57kg以下",
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        weight: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
      },
      frameNumber: "1",
      horseName: "Alpha",
      horseNumber: "1",
    },
  ]);
});

it("computes frame rates from win, quinella, and show counts when rate fields are missing", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      bloodlineRows: [],
      frameStats: [
        {
          ...frameOne,
          quinellaRate: Number.NaN,
          showRate: Number.NaN,
          winRate: Number.NaN,
        },
      ],
      horseResults: [],
      runners: [runnerOne],
      similarRows: [],
    }).map((row) => row.cells.frame),
  ).toStrictEqual([
    {
      name: "枠1",
      quinellaCount: 12,
      quinellaRate: 30,
      showCount: 18,
      showRate: 45,
      starts: 40,
      winCount: 6,
      winRate: 15,
    },
  ]);
});

it("computes frame rates from finish-position details when counts are missing", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      bloodlineRows: [],
      frameStats: [
        {
          ...frameOne,
          count: Number.NaN,
          details: [frameWinDetail, frameSecondDetail, frameThirdDetail, frameFifthDetail],
          quinellaCount: Number.NaN,
          quinellaRate: Number.NaN,
          showCount: Number.NaN,
          showRate: Number.NaN,
          winCount: Number.NaN,
          winRate: Number.NaN,
        },
      ],
      horseResults: [],
      runners: [runnerOne],
      similarRows: [],
    }).map((row) => row.cells.frame),
  ).toStrictEqual([
    {
      name: "枠1",
      quinellaCount: 2,
      quinellaRate: 50,
      showCount: 3,
      showRate: 75,
      starts: 4,
      winCount: 1,
      winRate: 25,
    },
  ]);
});

it("ignores frame stats whose frame number cannot be displayed", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      bloodlineRows: [],
      frameStats: [{ ...frameOne, frameNumber: "00" }],
      horseResults: [],
      runners: [runnerOne],
      similarRows: [],
    }),
  ).toStrictEqual([
    {
      cells: {
        damSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        frame: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        horse: {
          name: "Alpha",
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        jockey: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        trainer: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        carriedWeight: {
          name: "55.5kg以上57kg以下",
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        weight: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
      },
      frameNumber: "1",
      horseName: "Alpha",
      horseNumber: "1",
    },
  ]);
});

it("skips horse results whose current number cannot be displayed", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      bloodlineRows: [],
      frameStats: [],
      horseResults: [{ ...horseWin, currentUmaban: "00" }],
      runners: [runnerOne],
      similarRows: [],
    }),
  ).toStrictEqual([
    {
      cells: {
        carriedWeight: {
          name: "55.5kg以上57kg以下",
          quinellaCount: 1,
          quinellaRate: 100,
          showCount: 1,
          showRate: 100,
          starts: 1,
          winCount: 1,
          winRate: 100,
        },
        damSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        frame: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        horse: {
          name: "Alpha",
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        jockey: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        trainer: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        weight: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
      },
      frameNumber: "1",
      horseName: "Alpha",
      horseNumber: "1",
    },
  ]);
});

it("treats blank, zero, and non-numeric finish positions as missing horse rates", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      bloodlineRows: [],
      frameStats: [],
      horseResults: [
        { ...horseWin, kakuteiChakujun: "" },
        { ...horseWin, kakuteiChakujun: "00" },
        { ...horseWin, kakuteiChakujun: "外" },
      ],
      runners: [runnerOne],
      similarRows: [],
    }),
  ).toStrictEqual([
    {
      cells: {
        damSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        frame: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        horse: {
          name: "Alpha",
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        jockey: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        trainer: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        carriedWeight: {
          name: "55.5kg以上57kg以下",
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        weight: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
      },
      frameNumber: "1",
      horseName: "Alpha",
      horseNumber: "1",
    },
  ]);
});

it("uses a dash when the runner has no displayable horse name", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      bloodlineRows: [],
      frameStats: [],
      horseResults: [],
      runners: [{ ...runnerOne, bamei: "" }],
      similarRows: [],
    }),
  ).toStrictEqual([
    {
      cells: {
        damSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        frame: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        horse: {
          name: "-",
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        jockey: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        trainer: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        carriedWeight: {
          name: "55.5kg以上57kg以下",
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        weight: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
      },
      frameNumber: "1",
      horseName: "-",
      horseNumber: "1",
    },
  ]);
});

it("keeps heatmap row order stable when two formatted horse numbers are equal", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      bloodlineRows: [],
      frameStats: [],
      horseResults: [],
      runners: [
        { ...runnerTwo, umaban: "1", bamei: "Gamma" },
        { ...runnerOne, umaban: "01" },
      ],
      similarRows: [],
    }).map((row) => row.horseName),
  ).toStrictEqual(["Gamma", "Alpha"]);
});

it("maps heatmap view modes to win, quinella, show, or all three rate metrics", () => {
  expect(DEFAULT_WIN_RATE_HEATMAP_VIEW_MODE).toBe("winRate");
  expect(WIN_RATE_HEATMAP_VIEW_MODES).toStrictEqual([
    { key: "winRate", label: "勝率" },
    { key: "quinellaRate", label: "連対率" },
    { key: "showRate", label: "複勝率" },
    { key: "all", label: "勝率+連対率+複勝率" },
  ]);
  expect(getVisibleWinRateHeatmapRateMetrics("winRate")).toStrictEqual([
    { countKey: "winCount", hue: 8, key: "winRate", label: "勝率", shortLabel: "勝" },
  ]);
  expect(getVisibleWinRateHeatmapRateMetrics("quinellaRate")).toStrictEqual([
    { countKey: "quinellaCount", hue: 36, key: "quinellaRate", label: "連対率", shortLabel: "連" },
  ]);
  expect(getVisibleWinRateHeatmapRateMetrics("showRate")).toStrictEqual([
    { countKey: "showCount", hue: 196, key: "showRate", label: "複勝率", shortLabel: "複" },
  ]);
  expect(getVisibleWinRateHeatmapRateMetrics("all")).toStrictEqual([
    { countKey: "winCount", hue: 272, key: "winRate", label: "勝率", shortLabel: "勝" },
    { countKey: "quinellaCount", hue: 272, key: "quinellaRate", label: "連対率", shortLabel: "連" },
    { countKey: "showCount", hue: 272, key: "showRate", label: "複勝率", shortLabel: "複" },
  ]);
  expect(winRateHeatmapEntityColSpan(0)).toBe(1);
  expect(winRateHeatmapEntityColSpan(1)).toBe(1);
  expect(winRateHeatmapEntityColSpan(3)).toBe(3);
});

it("formats missing rates as a dash and numeric rates with one decimal", () => {
  expect(formatWinRateHeatmapValue(null, false)).toBe("-");
  expect(formatWinRateHeatmapValue(undefined, false)).toBe("-");
  expect(formatWinRateHeatmapValue(Number.NaN, false)).toBe("-");
  expect(formatWinRateHeatmapValue(12.5, false)).toBe("12.5%");
  expect(formatWinRateHeatmapValue(0, false)).toBe("0.0%");
});

it("omits the percent sign and writes zero without a decimal in the compact heatmap", () => {
  expect(formatWinRateHeatmapValue(null, true)).toBe("-");
  expect(formatWinRateHeatmapValue(undefined, true)).toBe("-");
  expect(formatWinRateHeatmapValue(Number.NaN, true)).toBe("-");
  expect(formatWinRateHeatmapValue(12.5, true)).toBe("12.5");
  expect(formatWinRateHeatmapValue(10, true)).toBe("10.0");
  expect(formatWinRateHeatmapValue(99.9, true)).toBe("99.9");
  expect(formatWinRateHeatmapValue(100, true)).toBe("100");
  expect(formatWinRateHeatmapValue(100.9, true)).toBe("100");
  expect(formatWinRateHeatmapValue(0, true)).toBe("0");
});

it("formats heatmap start counts as integers", () => {
  expect(formatWinRateHeatmapStarts(null)).toBe("-");
  expect(formatWinRateHeatmapStarts(undefined)).toBe("-");
  expect(formatWinRateHeatmapStarts(Number.NaN)).toBe("-");
  expect(formatWinRateHeatmapStarts(80)).toBe("80");
  expect(formatWinRateHeatmapStarts(0)).toBe("0");
  expect(formatWinRateHeatmapStarts(12.9)).toBe("12");
});

it("formats heatmap tooltip start counts in parentheses", () => {
  expect(formatWinRateHeatmapTooltipStarts(null)).toBe(null);
  expect(formatWinRateHeatmapTooltipStarts(undefined)).toBe(null);
  expect(formatWinRateHeatmapTooltipStarts(Number.NaN)).toBe(null);
  expect(formatWinRateHeatmapTooltipStarts(80)).toBe("(80)");
  expect(formatWinRateHeatmapTooltipStarts(0)).toBe("(0)");
  expect(formatWinRateHeatmapTooltipStarts(12.9)).toBe("(12)");
});

it("uses a dash when the heatmap tooltip name is missing", () => {
  expect(
    getWinRateHeatmapTooltipName({
      name: null,
      quinellaCount: null,
      quinellaRate: null,
      showCount: null,
      showRate: null,
      starts: null,
      winCount: null,
      winRate: null,
    }),
  ).toBe("-");
  expect(
    getWinRateHeatmapTooltipName({
      name: "Jockey A",
      quinellaCount: 20,
      quinellaRate: 25,
      showCount: 30,
      showRate: 37.5,
      starts: 80,
      winCount: 16,
      winRate: 20,
    }),
  ).toBe("Jockey A");
});

it("uses a gray background for missing rates and stronger color for higher rates", () => {
  expect(winRateHeatmapBackground(null, 8)).toBe("hsl(0, 0%, 96%)");
  expect(winRateHeatmapBackground(undefined, 8)).toBe("hsl(0, 0%, 96%)");
  expect(winRateHeatmapBackground(Number.NaN, 8)).toBe("hsl(0, 0%, 96%)");
  expect(winRateHeatmapBackground(0, 8)).toBe("hsl(8, 22%, 96%)");
  expect(winRateHeatmapBackground(10, 8)).toBe("hsl(8, 40%, 79%)");
  expect(winRateHeatmapBackground(20, 8)).toBe("hsl(8, 59%, 62%)");
  expect(winRateHeatmapBackground(40, 8)).toBe("hsl(8, 95%, 28%)");
  expect(winRateHeatmapBackground(100, 8)).toBe("hsl(8, 95%, 28%)");
  expect(winRateHeatmapBackground(20, 196)).toBe("hsl(196, 59%, 62%)");
  expect(winRateHeatmapBackground(-5, 8)).toBe("hsl(8, 22%, 96%)");
  expect(winRateHeatmapBackground(15, 272)).toBe("hsl(272, 49%, 71%)");
  expect(winRateHeatmapBackground(30, 272)).toBe("hsl(272, 77%, 45%)");
  expect(winRateHeatmapBackground(45, 272)).toBe("hsl(272, 95%, 28%)");
});

it("builds a horizontal color-scale gradient that matches heatmap cell colors", () => {
  expect(WIN_RATE_HEATMAP_COLOR_SCALE_MAX_RATE).toBe(40);
  expect(WIN_RATE_HEATMAP_COLOR_SCALE_TICKS).toStrictEqual([0, 10, 20, 30, 40]);
  expect(formatWinRateHeatmapColorScaleTick(0)).toBe("0%");
  expect(formatWinRateHeatmapColorScaleTick(39)).toBe("39%");
  expect(formatWinRateHeatmapColorScaleTick(40)).toBe("40%以上");
  expect(formatWinRateHeatmapColorScaleTick(50)).toBe("50%以上");
  expect(buildWinRateHeatmapColorScaleGradient(8)).toBe(
    "linear-gradient(to right, hsl(8, 22%, 96%) 0%, hsl(8, 40%, 79%) 25%, hsl(8, 59%, 62%) 50%, hsl(8, 77%, 45%) 75%, hsl(8, 95%, 28%) 100%)",
  );
  expect(buildWinRateHeatmapColorScaleGradient(196)).toBe(
    "linear-gradient(to right, hsl(196, 22%, 96%) 0%, hsl(196, 40%, 79%) 25%, hsl(196, 59%, 62%) 50%, hsl(196, 77%, 45%) 75%, hsl(196, 95%, 28%) 100%)",
  );
  expect(buildWinRateHeatmapColorScaleGradient(272)).toBe(
    "linear-gradient(to right, hsl(272, 22%, 96%) 0%, hsl(272, 40%, 79%) 25%, hsl(272, 59%, 62%) 50%, hsl(272, 77%, 45%) 75%, hsl(272, 95%, 28%) 100%)",
  );
});

it("names the color scale from visible metrics for single and combined views", () => {
  expect(formatWinRateHeatmapColorScaleCaption([])).toBe("");
  expect(
    formatWinRateHeatmapColorScaleCaption(getVisibleWinRateHeatmapRateMetrics("winRate")),
  ).toBe("勝率");
  expect(formatWinRateHeatmapColorScaleCaption(getVisibleWinRateHeatmapRateMetrics("all"))).toBe(
    "勝率+連対率+複勝率",
  );
  expect(formatWinRateHeatmapColorScaleAriaLabel([])).toBe("の色は0%から40%以上まで濃くなります");
  expect(
    formatWinRateHeatmapColorScaleAriaLabel(getVisibleWinRateHeatmapRateMetrics("winRate")),
  ).toBe("勝率の色は0%から40%以上まで濃くなります");
  expect(
    formatWinRateHeatmapColorScaleAriaLabel(getVisibleWinRateHeatmapRateMetrics("quinellaRate")),
  ).toBe("連対率の色は0%から40%以上まで濃くなります");
  expect(formatWinRateHeatmapColorScaleAriaLabel(getVisibleWinRateHeatmapRateMetrics("all"))).toBe(
    "勝率、連対率、複勝率の色は0%から40%以上まで濃くなります",
  );
});

it("collapses the color scale to one track when every visible metric shares a hue", () => {
  expect(getWinRateHeatmapColorScaleTracks([])).toStrictEqual([]);
  expect(
    getWinRateHeatmapColorScaleTracks(getVisibleWinRateHeatmapRateMetrics("winRate")),
  ).toStrictEqual([
    { countKey: "winCount", hue: 8, key: "winRate", label: "勝率", shortLabel: "勝" },
  ]);
  expect(
    getWinRateHeatmapColorScaleTracks(getVisibleWinRateHeatmapRateMetrics("all")),
  ).toStrictEqual([
    { countKey: "winCount", hue: 272, key: "winRate", label: "勝率", shortLabel: "勝" },
  ]);
  expect(
    getWinRateHeatmapColorScaleTracks([
      { countKey: "winCount", hue: 8, key: "winRate", label: "勝率", shortLabel: "勝" },
      {
        countKey: "quinellaCount",
        hue: 36,
        key: "quinellaRate",
        label: "連対率",
        shortLabel: "連",
      },
    ]),
  ).toStrictEqual([
    { countKey: "winCount", hue: 8, key: "winRate", label: "勝率", shortLabel: "勝" },
    { countKey: "quinellaCount", hue: 36, key: "quinellaRate", label: "連対率", shortLabel: "連" },
  ]);
});

it("hides the horse-weight column for overseas venues and when no runner has a weight", () => {
  expect(
    shouldShowWinRateHeatmapWeightColumn({
      keibajoCode: "A8",
      liveWeightKgByHorse: new Map(),
      runners: [{ ...runnerOne, bataiju: "480" }],
    }),
  ).toBe(false);
  expect(
    shouldShowWinRateHeatmapWeightColumn({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      runners: [runnerOne],
    }),
  ).toBe(false);
  expect(
    shouldShowWinRateHeatmapWeightColumn({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      runners: [{ ...runnerOne, bataiju: "000" }],
    }),
  ).toBe(false);
  expect(
    shouldShowWinRateHeatmapWeightColumn({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      runners: [{ ...runnerOne, bataiju: "480" }],
    }),
  ).toBe(true);
  expect(
    shouldShowWinRateHeatmapWeightColumn({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map([["1", 485]]),
      runners: [runnerOne],
    }),
  ).toBe(true);
});

it("hides the carried-weight column for overseas venues and when no runner has a declared 斤量", () => {
  expect(
    shouldShowWinRateHeatmapCarriedWeightColumn({
      keibajoCode: "A8",
      runners: [runnerOne],
    }),
  ).toBe(false);
  expect(
    shouldShowWinRateHeatmapCarriedWeightColumn({
      keibajoCode: "05",
      runners: [{ ...runnerOne, futanJuryo: null }],
    }),
  ).toBe(false);
  expect(
    shouldShowWinRateHeatmapCarriedWeightColumn({
      keibajoCode: "05",
      runners: [{ ...runnerOne, futanJuryo: "000" }],
    }),
  ).toBe(false);
  expect(
    shouldShowWinRateHeatmapCarriedWeightColumn({
      keibajoCode: "05",
      runners: [runnerOne],
    }),
  ).toBe(true);
});

it("hides the carried-weight column for ばんえい venues", () => {
  expect(
    shouldShowWinRateHeatmapCarriedWeightColumn({
      keibajoCode: "81",
      runners: [runnerOne],
    }),
  ).toBe(false);
  expect(
    shouldShowWinRateHeatmapCarriedWeightColumn({
      keibajoCode: "83",
      runners: [runnerOne],
    }),
  ).toBe(false);
});

it("maps a horse onto the 20kg weight-class rates computed from past races", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      bloodlineRows: [],
      frameStats: [],
      horseResults: [
        { ...horseWin, bataiju: "480", kakuteiChakujun: "01" },
        { ...horseSecond, bataiju: "490", kakuteiChakujun: "02" },
        { ...horseWin, bataiju: "485", kakuteiChakujun: "05" },
        { ...horseWin, bataiju: "510", kakuteiChakujun: "01" },
        { ...horseWin, bataiju: "000", kakuteiChakujun: "01" },
        { ...horseWin, bataiju: "480", kakuteiChakujun: "00" },
      ],
      runners: [{ ...runnerOne, bataiju: "485" }],
      similarRows: [],
    }).map((row) => row.cells.weight),
  ).toStrictEqual([
    {
      name: "480-499kg",
      quinellaCount: 2,
      quinellaRate: 66.7,
      showCount: 2,
      showRate: 66.7,
      starts: 3,
      winCount: 1,
      winRate: 33.3,
    },
  ]);
});

it("keeps the weight-class label when the current horse has a weight but that class has no past starts", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      bloodlineRows: [],
      frameStats: [],
      horseResults: [{ ...horseWin, bataiju: "480", kakuteiChakujun: "01" }],
      runners: [{ ...runnerOne, bataiju: "399" }],
      similarRows: [],
    }).map((row) => row.cells.weight),
  ).toStrictEqual([
    {
      name: "399kg以下",
      quinellaCount: null,
      quinellaRate: null,
      showCount: null,
      showRate: null,
      starts: null,
      winCount: null,
      winRate: null,
    },
  ]);
});

it("uses a live kilogram weight when stored bataiju is still empty", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map([["1", 485]]),
      bloodlineRows: [],
      frameStats: [],
      horseResults: [{ ...horseWin, bataiju: "480", kakuteiChakujun: "01" }],
      runners: [runnerOne],
      similarRows: [],
    }).map((row) => row.cells.weight),
  ).toStrictEqual([
    {
      name: "480-499kg",
      quinellaCount: 1,
      quinellaRate: 100,
      showCount: 1,
      showRate: 100,
      starts: 1,
      winCount: 1,
      winRate: 100,
    },
  ]);
});

it("maps a horse onto similar-race carried-weight class rates", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      bloodlineRows: [],
      carriedWeightClassStats: [
        {
          key: "55.5-57",
          quinellaCount: 20,
          quinellaRate: 25,
          showCount: 32,
          showRate: 40,
          starts: 80,
          winCount: 12,
          winRate: 15,
        },
      ],
      frameStats: [],
      horseResults: [{ ...horseWin, futanJuryo: "570", kakuteiChakujun: "05" }],
      runners: [runnerOne],
      similarRows: [],
    }).map((row) => row.cells.carriedWeight),
  ).toStrictEqual([
    {
      name: "55.5kg以上57kg以下",
      quinellaCount: 20,
      quinellaRate: 25,
      showCount: 32,
      showRate: 40,
      starts: 80,
      winCount: 12,
      winRate: 15,
    },
  ]);
});

it("uses similar-race weight-class rates instead of this field's past results", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "48",
      liveWeightKgByHorse: new Map([["1", 453]]),
      bloodlineRows: [],
      frameStats: [],
      horseResults: [{ ...horseWin, bataiju: "450", kakuteiChakujun: "05" }],
      runners: [{ ...runnerOne, bataiju: "   " }],
      similarRows: [],
      weightClassStats: [
        {
          key: "440-459",
          quinellaCount: 20,
          quinellaRate: 25,
          showCount: 32,
          showRate: 40,
          starts: 80,
          winCount: 12,
          winRate: 15,
        },
      ],
    }).map((row) => row.cells.weight),
  ).toStrictEqual([
    {
      name: "440-459kg",
      quinellaCount: 20,
      quinellaRate: 25,
      showCount: 32,
      showRate: 40,
      starts: 80,
      winCount: 12,
      winRate: 15,
    },
  ]);
});

it("skips similar-race weight-class rows that have no starts", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "48",
      liveWeightKgByHorse: new Map([["1", 453]]),
      bloodlineRows: [],
      frameStats: [],
      horseResults: [],
      runners: [{ ...runnerOne, bataiju: "   " }],
      similarRows: [],
      weightClassStats: [
        {
          key: "440-459",
          quinellaCount: 0,
          quinellaRate: null,
          showCount: 0,
          showRate: null,
          starts: 0,
          winCount: 0,
          winRate: null,
        },
      ],
    }).map((row) => row.cells.weight),
  ).toStrictEqual([
    {
      name: "440-459kg",
      quinellaCount: null,
      quinellaRate: null,
      showCount: null,
      showRate: null,
      starts: null,
      winCount: null,
      winRate: null,
    },
  ]);
});

it("leaves the weight cell empty when the current horse has no published weight", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      bloodlineRows: [],
      frameStats: [],
      horseResults: [{ ...horseWin, bataiju: "480", kakuteiChakujun: "01" }],
      runners: [runnerOne],
      similarRows: [],
    }).map((row) => row.cells.weight),
  ).toStrictEqual([
    {
      name: null,
      quinellaCount: null,
      quinellaRate: null,
      showCount: null,
      showRate: null,
      starts: null,
      winCount: null,
      winRate: null,
    },
  ]);
});

it("skips similar and bloodline rows whose current horse numbers are blank", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      bloodlineRows: [{ ...bloodlineSire, currentHorseNumbers: "" }],
      frameStats: [],
      horseResults: [],
      runners: [runnerOne],
      similarRows: [{ ...similarJockey, currentHorseNumbers: "   " }],
    }).map((row) => ({
      horse: row.cells.horse.name,
      jockey: row.cells.jockey.name,
      sire: row.cells.sire.name,
    })),
  ).toStrictEqual([
    {
      horse: "Alpha",
      jockey: null,
      sire: null,
    },
  ]);
});

it("decodes Ban-ei hex horse weights when classifying heatmap weight cells", () => {
  expect(
    buildWinRateHeatmapRows({
      keibajoCode: "83",
      liveWeightKgByHorse: new Map(),
      bloodlineRows: [],
      frameStats: [],
      horseResults: [
        { ...horseWin, bataiju: "4AE", kakuteiChakujun: "01", keibajoCode: "83" },
        { ...horseSecond, bataiju: "4AE", kakuteiChakujun: "03", keibajoCode: "83" },
      ],
      runners: [{ ...runnerOne, bataiju: "4AE" }],
      similarRows: [],
    }).map((row) => row.cells.weight),
  ).toStrictEqual([
    {
      name: "540kg以上",
      quinellaCount: 1,
      quinellaRate: 50,
      showCount: 2,
      showRate: 100,
      starts: 2,
      winCount: 1,
      winRate: 50,
    },
  ]);
});
