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
  buildWinRateHeatmapDisplay,
  buildWinRateHeatmapRows,
  DEFAULT_WIN_RATE_HEATMAP_VIEW_MODE,
  formatWinRateHeatmapColorScaleAriaLabel,
  formatWinRateHeatmapColorScaleCaption,
  formatWinRateHeatmapColorScaleTick,
  formatWinRateHeatmapGraphStarts,
  formatWinRateHeatmapStarts,
  formatWinRateHeatmapTooltipStarts,
  formatWinRateHeatmapValue,
  getVisibleWinRateHeatmapColumns,
  shouldShowWinRateHeatmapCarriedWeightColumn,
  getVisibleWinRateHeatmapRateMetrics,
  getWinRateHeatmapColorScale,
  getWinRateHeatmapColorScaleTracks,
  resolveWinRateHeatmapColorScale,
  getWinRateHeatmapTooltipName,
  shouldShowWinRateHeatmapWeightColumn,
  BAN_EI_WIN_RATE_HEATMAP_QUINELLA_MAX_RATE,
  BAN_EI_WIN_RATE_HEATMAP_QUINELLA_TICKS,
  BAN_EI_WIN_RATE_HEATMAP_SHOW_MAX_RATE,
  BAN_EI_WIN_RATE_HEATMAP_SHOW_TICKS,
  BAN_EI_WIN_RATE_HEATMAP_WIN_MAX_RATE,
  BAN_EI_WIN_RATE_HEATMAP_WIN_TICKS,
  WIN_RATE_HEATMAP_COLOR_SCALE_MAX_RATE,
  WIN_RATE_HEATMAP_COLOR_SCALE_TICKS,
  WIN_RATE_HEATMAP_COLUMNS,
  WIN_RATE_HEATMAP_COMBINED_HUE,
  WIN_RATE_HEATMAP_RATE_METRICS,
  WIN_RATE_HEATMAP_VIEW_MODES,
  winRateHeatmapBackground,
  winRateHeatmapEntityColSpan,
  winRateHeatmapForeground,
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

const bloodlineSireDamSire: BloodlineStatsRow = {
  category: "sireDamSire",
  currentHorseNumbers: "2",
  details: [],
  horseCount: 22,
  name: "Sire Dam Sire Beta",
  quinellaCount: 30,
  quinellaRate: 15,
  showCount: 50,
  showRate: 25,
  starts: 200,
  winCount: 10,
  winRate: 5,
};

const bloodlineSireSireSire: BloodlineStatsRow = {
  category: "sireSireSire",
  currentHorseNumbers: "2",
  details: [],
  horseCount: 40,
  name: "Sire Sire Sire Beta",
  quinellaCount: 60,
  quinellaRate: 12,
  showCount: 90,
  showRate: 18,
  starts: 500,
  winCount: 20,
  winRate: 4,
};

const bloodlineDamSireSire: BloodlineStatsRow = {
  category: "damSireSire",
  currentHorseNumbers: "2",
  details: [],
  horseCount: 16,
  name: "Dam Sire Sire Beta",
  quinellaCount: 18,
  quinellaRate: 9,
  showCount: 32,
  showRate: 16,
  starts: 200,
  winCount: 6,
  winRate: 3,
};

const bloodlineDamDamSire: BloodlineStatsRow = {
  category: "damDamSire",
  currentHorseNumbers: "2",
  details: [],
  horseCount: 12,
  name: "Dam Dam Sire Beta",
  quinellaCount: 10,
  quinellaRate: 8,
  showCount: 20,
  showRate: 16,
  starts: 125,
  winCount: 4,
  winRate: 3.2,
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
    { key: "jockeyFrame", label: "騎手枠別" },
    { key: "jockey", label: "騎手" },
    { key: "trainer", label: "調教師" },
    { key: "sire", label: "父" },
    { key: "damSire", label: "母父" },
    { key: "sireSire", label: "父父" },
    { key: "sireDamSire", label: "父母父" },
    { key: "sireSireSire", label: "父父父" },
    { key: "damSireSire", label: "母父父" },
    { key: "damDamSire", label: "母母父" },
  ]);
  expect(
    getVisibleWinRateHeatmapColumns({
      keibajoCode: "05",
      showCarriedWeight: true,
      showWeight: true,
    }),
  ).toStrictEqual([
    { key: "frame", label: "枠" },
    { key: "weight", label: "馬体重" },
    { key: "carriedWeight", label: "斤量" },
    { key: "horse", label: "馬" },
    { key: "jockeyFrame", label: "騎手枠別" },
    { key: "jockey", label: "騎手" },
    { key: "trainer", label: "調教師" },
    { key: "sire", label: "父" },
    { key: "damSire", label: "母父" },
    { key: "sireSire", label: "父父" },
    { key: "sireDamSire", label: "父母父" },
    { key: "sireSireSire", label: "父父父" },
    { key: "damSireSire", label: "母父父" },
    { key: "damDamSire", label: "母母父" },
  ]);
  expect(
    getVisibleWinRateHeatmapColumns({
      keibajoCode: "05",
      showCarriedWeight: false,
      showWeight: false,
    }),
  ).toStrictEqual([
    { key: "frame", label: "枠" },
    { key: "horse", label: "馬" },
    { key: "jockeyFrame", label: "騎手枠別" },
    { key: "jockey", label: "騎手" },
    { key: "trainer", label: "調教師" },
    { key: "sire", label: "父" },
    { key: "damSire", label: "母父" },
    { key: "sireSire", label: "父父" },
    { key: "sireDamSire", label: "父母父" },
    { key: "sireSireSire", label: "父父父" },
    { key: "damSireSire", label: "母父父" },
    { key: "damDamSire", label: "母母父" },
  ]);
});

it("limits ばんえい heatmap bloodline columns to 父 and 母父", () => {
  expect(
    getVisibleWinRateHeatmapColumns({
      keibajoCode: "83",
      showCarriedWeight: false,
      showWeight: true,
    }),
  ).toStrictEqual([
    { key: "frame", label: "枠" },
    { key: "weight", label: "馬体重" },
    { key: "horse", label: "馬" },
    { key: "jockeyFrame", label: "騎手枠別" },
    { key: "jockey", label: "騎手" },
    { key: "trainer", label: "調教師" },
    { key: "sire", label: "父" },
    { key: "damSire", label: "母父" },
  ]);
});

it("places 騎手枠別 immediately left of 騎手 in the heatmap column order", () => {
  expect(WIN_RATE_HEATMAP_COLUMNS[3]).toStrictEqual({ key: "horse", label: "馬" });
  expect(WIN_RATE_HEATMAP_COLUMNS[4]).toStrictEqual({ key: "jockeyFrame", label: "騎手枠別" });
  expect(WIN_RATE_HEATMAP_COLUMNS[5]).toStrictEqual({ key: "jockey", label: "騎手" });
});

it("maps jockeyFrame similar stats onto 騎手枠別 instead of reusing generic jockey stats", () => {
  expect(
    buildWinRateHeatmapRows({
      bloodlineRows: [],
      frameStats: [],
      horseResults: [],
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      runners: [runnerOne],
      similarRows: [
        {
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
        },
        {
          category: "jockeyFrame",
          currentHorseNumbers: "1",
          details: [],
          horseCount: 0,
          name: "Jockey A",
          quinellaCount: 4,
          quinellaRate: 40,
          showCount: 5,
          showRate: 50,
          starts: 10,
          winCount: 2,
          winRate: 20,
        },
      ],
    })[0]?.cells,
  ).toStrictEqual({
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
    damDamSire: {
      name: null,
      quinellaCount: null,
      quinellaRate: null,
      showCount: null,
      showRate: null,
      starts: null,
      winCount: null,
      winRate: null,
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
    damSireSire: {
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
      name: "Jockey A",
      quinellaCount: 20,
      quinellaRate: 25,
      showCount: 30,
      showRate: 37.5,
      starts: 80,
      winCount: 16,
      winRate: 20,
    },
    jockeyFrame: {
      name: "Jockey A",
      quinellaCount: 4,
      quinellaRate: 40,
      showCount: 5,
      showRate: 50,
      starts: 10,
      winCount: 2,
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
    sireDamSire: {
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
    sireSireSire: {
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
  });
});

it("keeps 騎手枠別 empty when similarRows omit jockeyFrame", () => {
  expect(
    buildWinRateHeatmapRows({
      bloodlineRows: [],
      frameStats: [],
      horseResults: [],
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      runners: [runnerOne],
      similarRows: [
        {
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
        },
      ],
    })[0]?.cells.jockeyFrame,
  ).toStrictEqual({
    name: null,
    quinellaCount: null,
    quinellaRate: null,
    showCount: null,
    showRate: null,
    starts: null,
    winCount: null,
    winRate: null,
  });
});

it("keeps 騎手枠別 visible for ばんえい races", () => {
  expect(
    getVisibleWinRateHeatmapColumns({
      keibajoCode: "83",
      showCarriedWeight: false,
      showWeight: false,
    }),
  ).toStrictEqual([
    { key: "frame", label: "枠" },
    { key: "horse", label: "馬" },
    { key: "jockeyFrame", label: "騎手枠別" },
    { key: "jockey", label: "騎手" },
    { key: "trainer", label: "調教師" },
    { key: "sire", label: "父" },
    { key: "damSire", label: "母父" },
  ]);
});

it("keeps per-umaban jockeyFrame rates when two horses share a jockey in different frames", () => {
  expect(
    buildWinRateHeatmapRows({
      bloodlineRows: [],
      frameStats: [],
      horseResults: [],
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
      runners: [
        runnerOne,
        {
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
          kishumeiRyakusho: "Jockey A",
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
        },
      ],
      similarRows: [
        {
          category: "jockey",
          currentHorseNumbers: "1, 2",
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
        },
        {
          category: "jockeyFrame",
          currentHorseNumbers: "1",
          details: [],
          horseCount: 0,
          name: "Jockey A",
          quinellaCount: 1,
          quinellaRate: 10,
          showCount: 2,
          showRate: 20,
          starts: 10,
          winCount: 1,
          winRate: 10,
        },
        {
          category: "jockeyFrame",
          currentHorseNumbers: "2",
          details: [],
          horseCount: 0,
          name: "Jockey A",
          quinellaCount: 6,
          quinellaRate: 60,
          showCount: 7,
          showRate: 70,
          starts: 10,
          winCount: 3,
          winRate: 30,
        },
      ],
    }).map((row) => ({
      horseNumber: row.horseNumber,
      jockeyFrame: row.cells.jockeyFrame,
    })),
  ).toStrictEqual([
    {
      horseNumber: "1",
      jockeyFrame: {
        name: "Jockey A",
        quinellaCount: 1,
        quinellaRate: 10,
        showCount: 2,
        showRate: 20,
        starts: 10,
        winCount: 1,
        winRate: 10,
      },
    },
    {
      horseNumber: "2",
      jockeyFrame: {
        name: "Jockey A",
        quinellaCount: 6,
        quinellaRate: 60,
        showCount: 7,
        showRate: 70,
        starts: 10,
        winCount: 3,
        winRate: 30,
      },
    },
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
      bloodlineRows: [
        bloodlineSire,
        bloodlineDamSire,
        bloodlineSireSire,
        bloodlineSireDamSire,
        bloodlineSireSireSire,
        bloodlineDamSireSire,
      ],
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
        jockeyFrame: {
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
        sireDamSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sireSireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        damSireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        damDamSire: {
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
        jockeyFrame: {
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
        sireDamSire: {
          name: "Sire Dam Sire Beta",
          quinellaCount: 30,
          quinellaRate: 15,
          showCount: 50,
          showRate: 25,
          starts: 200,
          winCount: 10,
          winRate: 5,
        },
        sireSireSire: {
          name: "Sire Sire Sire Beta",
          quinellaCount: 60,
          quinellaRate: 12,
          showCount: 90,
          showRate: 18,
          starts: 500,
          winCount: 20,
          winRate: 4,
        },
        damSireSire: {
          name: "Dam Sire Sire Beta",
          quinellaCount: 18,
          quinellaRate: 9,
          showCount: 32,
          showRate: 16,
          starts: 200,
          winCount: 6,
          winRate: 3,
        },
        damDamSire: {
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
        jockeyFrame: {
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
        sireDamSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sireSireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        damSireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        damDamSire: {
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
        jockeyFrame: {
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
        sireDamSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sireSireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        damSireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        damDamSire: {
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
        jockeyFrame: {
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
        sireDamSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sireSireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        damSireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        damDamSire: {
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
        jockeyFrame: {
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
        sireDamSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sireSireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        damSireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        damDamSire: {
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
        jockeyFrame: {
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
        sireDamSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sireSireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        damSireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        damDamSire: {
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
        jockeyFrame: {
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
        sireDamSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        sireSireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        damSireSire: {
          name: null,
          quinellaCount: null,
          quinellaRate: null,
          showCount: null,
          showRate: null,
          starts: null,
          winCount: null,
          winRate: null,
        },
        damDamSire: {
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
  expect(formatWinRateHeatmapValue(null)).toBe("-");
  expect(formatWinRateHeatmapValue(undefined)).toBe("-");
  expect(formatWinRateHeatmapValue(Number.NaN)).toBe("-");
  expect(formatWinRateHeatmapValue(12.5)).toBe("12.5");
  expect(formatWinRateHeatmapValue(0)).toBe("0.0");
  expect(formatWinRateHeatmapValue(100)).toBe("100.0");
  expect(formatWinRateHeatmapValue(100.9)).toBe("100.9");
  expect(formatWinRateHeatmapValue(15)).toBe("15.0");
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
  expect(formatWinRateHeatmapTooltipStarts(1234)).toBe("(1234)");
});

it("wraps graph start counts in parentheses for every heatmap view", () => {
  expect(formatWinRateHeatmapGraphStarts(null)).toBe(null);
  expect(formatWinRateHeatmapGraphStarts(undefined)).toBe(null);
  expect(formatWinRateHeatmapGraphStarts(Number.NaN)).toBe(null);
  expect(formatWinRateHeatmapGraphStarts(80)).toBe("(80)");
  expect(formatWinRateHeatmapGraphStarts(0)).toBe("(0)");
  expect(formatWinRateHeatmapGraphStarts(1234)).toBe("(1234)");
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

it("uses extra bloodline names in heatmap tooltips for 父母父, 父父父, 母父父, and 母母父", () => {
  expect(
    getWinRateHeatmapTooltipName({
      name: "Sire Dam Sire Beta",
      quinellaCount: 30,
      quinellaRate: 15,
      showCount: 50,
      showRate: 25,
      starts: 200,
      winCount: 10,
      winRate: 5,
    }),
  ).toBe("Sire Dam Sire Beta");
  expect(
    getWinRateHeatmapTooltipName({
      name: "Sire Sire Sire Beta",
      quinellaCount: 60,
      quinellaRate: 12,
      showCount: 90,
      showRate: 18,
      starts: 500,
      winCount: 20,
      winRate: 4,
    }),
  ).toBe("Sire Sire Sire Beta");
  expect(
    getWinRateHeatmapTooltipName({
      name: "Dam Sire Sire Beta",
      quinellaCount: 18,
      quinellaRate: 9,
      showCount: 32,
      showRate: 16,
      starts: 200,
      winCount: 6,
      winRate: 3,
    }),
  ).toBe("Dam Sire Sire Beta");
  expect(
    getWinRateHeatmapTooltipName({
      name: "Dam Dam Sire Beta",
      quinellaCount: 10,
      quinellaRate: 8,
      showCount: 20,
      showRate: 16,
      starts: 125,
      winCount: 4,
      winRate: 3.2,
    }),
  ).toBe("Dam Dam Sire Beta");
  const display = buildWinRateHeatmapDisplay({
    bloodlineRows: [
      bloodlineSireDamSire,
      bloodlineSireSireSire,
      bloodlineDamSireSire,
      bloodlineDamDamSire,
    ],
    frameStats: [],
    horseResults: [],
    keibajoCode: "05",
    liveWeightKgByHorse: new Map(),
    runners: [runnerTwo],
    showStarts: false,
    similarRows: [],
    viewMode: "winRate",
  });
  const sireDamSireSwatch = display.rows[0]?.swatches.find(
    (swatch) => swatch.columnKey === "sireDamSire" && swatch.metricKey === "winRate",
  );
  const sireSireSireSwatch = display.rows[0]?.swatches.find(
    (swatch) => swatch.columnKey === "sireSireSire" && swatch.metricKey === "winRate",
  );
  const damSireSireSwatch = display.rows[0]?.swatches.find(
    (swatch) => swatch.columnKey === "damSireSire" && swatch.metricKey === "winRate",
  );
  expect(sireDamSireSwatch?.columnLabel).toBe("父母父");
  expect(sireDamSireSwatch?.name).toBe("Sire Dam Sire Beta");
  expect(sireDamSireSwatch?.valueLabel).toBe("5.0");
  expect(sireSireSireSwatch?.columnLabel).toBe("父父父");
  expect(sireSireSireSwatch?.name).toBe("Sire Sire Sire Beta");
  expect(sireSireSireSwatch?.valueLabel).toBe("4.0");
  expect(damSireSireSwatch?.columnLabel).toBe("母父父");
  expect(damSireSireSwatch?.name).toBe("Dam Sire Sire Beta");
  expect(damSireSireSwatch?.valueLabel).toBe("3.0");
  const damDamSireSwatch = display.rows[0]?.swatches.find(
    (swatch) => swatch.columnKey === "damDamSire" && swatch.metricKey === "winRate",
  );
  expect(damDamSireSwatch?.columnLabel).toBe("母母父");
  expect(damDamSireSwatch?.name).toBe("Dam Dam Sire Beta");
  expect(damDamSireSwatch?.valueLabel).toBe("3.2");
});

it("uses the ばんえい color scale on heatmap cells and hides extra bloodline columns", () => {
  const display = buildWinRateHeatmapDisplay({
    bloodlineRows: [
      {
        category: "sire",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 1,
        name: "BanEi Sire",
        quinellaCount: 40,
        quinellaRate: 40,
        showCount: 40,
        showRate: 40,
        starts: 100,
        winCount: 40,
        winRate: 40,
      },
    ],
    frameStats: [],
    horseResults: [],
    keibajoCode: "83",
    liveWeightKgByHorse: new Map(),
    runners: [runnerOne],
    showStarts: false,
    similarRows: [],
    viewMode: "winRate",
  });
  expect(display.visibleColumns).toStrictEqual([
    { key: "frame", label: "枠" },
    { key: "horse", label: "馬" },
    { key: "jockeyFrame", label: "騎手枠別" },
    { key: "jockey", label: "騎手" },
    { key: "trainer", label: "調教師" },
    { key: "sire", label: "父" },
    { key: "damSire", label: "母父" },
  ]);
  const sireSwatch = display.rows[0]?.swatches.find(
    (swatch) => swatch.columnKey === "sire" && swatch.metricKey === "winRate",
  );
  expect(sireSwatch?.background).toBe("hsl(8, 73%, 50%)");
  expect(sireSwatch?.foreground).toBe("var(--surface)");
  expect(display.rows[0]?.swatches.find((swatch) => swatch.columnKey === "sireSire")).toBe(
    undefined,
  );
});

it("stretches ばんえい heatmap cells from the lowest table rate to the highest", () => {
  const display = buildWinRateHeatmapDisplay({
    bloodlineRows: [
      {
        category: "sire",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 1,
        name: "BanEi Sire",
        quinellaCount: 8,
        quinellaRate: 8,
        showCount: 8,
        showRate: 8,
        starts: 100,
        winCount: 8,
        winRate: 8,
      },
    ],
    frameStats: [],
    horseResults: [],
    keibajoCode: "83",
    liveWeightKgByHorse: new Map(),
    runners: [runnerOne],
    showStarts: false,
    similarRows: [
      {
        category: "jockey",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 1,
        name: "Jockey A",
        quinellaCount: 16,
        quinellaRate: 16,
        showCount: 16,
        showRate: 16,
        starts: 80,
        winCount: 16,
        winRate: 16,
      },
    ],
    viewMode: "winRate",
  });
  const sireSwatch = display.rows[0]?.swatches.find(
    (swatch) => swatch.columnKey === "sire" && swatch.metricKey === "winRate",
  );
  const jockeySwatch = display.rows[0]?.swatches.find(
    (swatch) => swatch.columnKey === "jockey" && swatch.metricKey === "winRate",
  );
  expect(sireSwatch?.background).toBe("hsl(8, 45%, 86%)");
  expect(jockeySwatch?.background).toBe("hsl(8, 100%, 14%)");
  expect(display.colorScales.winRate).toStrictEqual({
    maxRate: 16,
    minRate: 8,
    ticks: [8, 10, 12, 14, 16],
  });
});

it("uses a gray background for missing rates and stronger color for higher rates", () => {
  expect(winRateHeatmapBackground({ hue: 8, maxRate: 40, minRate: 0, rate: null })).toBe(
    "hsl(0, 0%, 96%)",
  );
  expect(winRateHeatmapBackground({ hue: 8, maxRate: 40, minRate: 0, rate: undefined })).toBe(
    "hsl(0, 0%, 96%)",
  );
  expect(winRateHeatmapBackground({ hue: 8, maxRate: 40, minRate: 0, rate: Number.NaN })).toBe(
    "hsl(0, 0%, 96%)",
  );
  expect(winRateHeatmapBackground({ hue: 8, maxRate: 40, minRate: 0, rate: 0 })).toBe(
    "hsl(8, 22%, 96%)",
  );
  expect(winRateHeatmapBackground({ hue: 8, maxRate: 40, minRate: 0, rate: 10 })).toBe(
    "hsl(8, 40%, 79%)",
  );
  expect(winRateHeatmapBackground({ hue: 8, maxRate: 40, minRate: 0, rate: 20 })).toBe(
    "hsl(8, 59%, 62%)",
  );
  expect(winRateHeatmapBackground({ hue: 8, maxRate: 40, minRate: 0, rate: 40 })).toBe(
    "hsl(8, 95%, 28%)",
  );
  expect(winRateHeatmapBackground({ hue: 8, maxRate: 40, minRate: 0, rate: 100 })).toBe(
    "hsl(8, 95%, 28%)",
  );
  expect(winRateHeatmapBackground({ hue: 196, maxRate: 40, minRate: 0, rate: 20 })).toBe(
    "hsl(196, 59%, 62%)",
  );
  expect(winRateHeatmapBackground({ hue: 8, maxRate: 40, minRate: 0, rate: -5 })).toBe(
    "hsl(8, 22%, 96%)",
  );
  expect(winRateHeatmapBackground({ hue: 272, maxRate: 40, minRate: 0, rate: 15 })).toBe(
    "hsl(272, 49%, 71%)",
  );
  expect(winRateHeatmapBackground({ hue: 272, maxRate: 40, minRate: 0, rate: 30 })).toBe(
    "hsl(272, 77%, 45%)",
  );
  expect(winRateHeatmapBackground({ hue: 272, maxRate: 40, minRate: 0, rate: 45 })).toBe(
    "hsl(272, 95%, 28%)",
  );
});

it("maps the lowest and highest ばんえい rates onto the full color range", () => {
  expect(winRateHeatmapBackground({ hue: 8, maxRate: 16, minRate: 8, rate: 8 })).toBe(
    "hsl(8, 45%, 86%)",
  );
  expect(winRateHeatmapBackground({ hue: 8, maxRate: 16, minRate: 8, rate: 16 })).toBe(
    "hsl(8, 100%, 14%)",
  );
  expect(winRateHeatmapForeground({ hue: 8, maxRate: 16, minRate: 8, rate: 8 })).toBe("var(--ink)");
  expect(winRateHeatmapForeground({ hue: 8, maxRate: 16, minRate: 8, rate: 16 })).toBe(
    "var(--surface)",
  );
});

it("maps clustered ばんえい show rates onto the full color range", () => {
  expect(winRateHeatmapBackground({ hue: 196, maxRate: 40, minRate: 24, rate: 24 })).toBe(
    "hsl(196, 45%, 86%)",
  );
  expect(winRateHeatmapBackground({ hue: 196, maxRate: 40, minRate: 24, rate: 40 })).toBe(
    "hsl(196, 100%, 14%)",
  );
});

it("picks dark or light heatmap text from the fill contrast", () => {
  expect(winRateHeatmapForeground({ hue: 8, maxRate: 40, minRate: 0, rate: null })).toBe(
    "var(--ink)",
  );
  expect(winRateHeatmapForeground({ hue: 8, maxRate: 40, minRate: 0, rate: undefined })).toBe(
    "var(--ink)",
  );
  expect(winRateHeatmapForeground({ hue: 8, maxRate: 40, minRate: 0, rate: Number.NaN })).toBe(
    "var(--ink)",
  );
  expect(winRateHeatmapForeground({ hue: 8, maxRate: 40, minRate: 0, rate: 0 })).toBe("var(--ink)");
  expect(winRateHeatmapForeground({ hue: 8, maxRate: 40, minRate: 0, rate: 20 })).toBe(
    "var(--ink)",
  );
  expect(winRateHeatmapForeground({ hue: 8, maxRate: 40, minRate: 0, rate: 30 })).toBe(
    "var(--surface)",
  );
  expect(winRateHeatmapForeground({ hue: 8, maxRate: 40, minRate: 0, rate: 40 })).toBe(
    "var(--surface)",
  );
  expect(winRateHeatmapForeground({ hue: 8, maxRate: 40, minRate: 0, rate: 100 })).toBe(
    "var(--surface)",
  );
  expect(winRateHeatmapForeground({ hue: 272, maxRate: 40, minRate: 0, rate: 15 })).toBe(
    "var(--ink)",
  );
  expect(winRateHeatmapForeground({ hue: 272, maxRate: 40, minRate: 0, rate: 30 })).toBe(
    "var(--surface)",
  );
  expect(winRateHeatmapForeground({ hue: 272, maxRate: 40, minRate: 0, rate: 45 })).toBe(
    "var(--surface)",
  );
});

it("builds a horizontal color-scale gradient that matches heatmap cell colors", () => {
  expect(WIN_RATE_HEATMAP_COLOR_SCALE_MAX_RATE).toBe(40);
  expect(WIN_RATE_HEATMAP_COLOR_SCALE_TICKS).toStrictEqual([0, 10, 20, 30, 40]);
  expect(formatWinRateHeatmapColorScaleTick(0, 40)).toBe("0%");
  expect(formatWinRateHeatmapColorScaleTick(39, 40)).toBe("39%");
  expect(formatWinRateHeatmapColorScaleTick(40, 40)).toBe("40%以上");
  expect(formatWinRateHeatmapColorScaleTick(50, 40)).toBe("50%以上");
  expect(
    buildWinRateHeatmapColorScaleGradient({
      hue: 8,
      maxRate: 40,
      minRate: 0,
      ticks: [0, 10, 20, 30, 40],
    }),
  ).toBe(
    "linear-gradient(to right, hsl(8, 22%, 96%) 0%, hsl(8, 40%, 79%) 25%, hsl(8, 59%, 62%) 50%, hsl(8, 77%, 45%) 75%, hsl(8, 95%, 28%) 100%)",
  );
  expect(
    buildWinRateHeatmapColorScaleGradient({
      hue: 196,
      maxRate: 40,
      minRate: 0,
      ticks: [0, 10, 20, 30, 40],
    }),
  ).toBe(
    "linear-gradient(to right, hsl(196, 22%, 96%) 0%, hsl(196, 40%, 79%) 25%, hsl(196, 59%, 62%) 50%, hsl(196, 77%, 45%) 75%, hsl(196, 95%, 28%) 100%)",
  );
  expect(
    buildWinRateHeatmapColorScaleGradient({
      hue: 272,
      maxRate: 40,
      minRate: 0,
      ticks: [0, 10, 20, 30, 40],
    }),
  ).toBe(
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
  expect(
    formatWinRateHeatmapColorScaleAriaLabel([], {
      quinellaRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
      showRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
      winRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
    }),
  ).toBe("の色は0%から40%以上まで濃くなります");
  expect(
    formatWinRateHeatmapColorScaleAriaLabel(getVisibleWinRateHeatmapRateMetrics("winRate"), {
      quinellaRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
      showRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
      winRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
    }),
  ).toBe("勝率の色は0%から40%以上まで濃くなります");
  expect(
    formatWinRateHeatmapColorScaleAriaLabel(getVisibleWinRateHeatmapRateMetrics("quinellaRate"), {
      quinellaRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
      showRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
      winRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
    }),
  ).toBe("連対率の色は0%から40%以上まで濃くなります");
  expect(
    formatWinRateHeatmapColorScaleAriaLabel(getVisibleWinRateHeatmapRateMetrics("all"), {
      quinellaRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
      showRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
      winRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
    }),
  ).toBe("勝率、連対率、複勝率の色は0%から40%以上まで濃くなります");
  expect(
    formatWinRateHeatmapColorScaleAriaLabel(getVisibleWinRateHeatmapRateMetrics("winRate"), {
      quinellaRate: { maxRate: 40, minRate: 16, ticks: [16, 22, 28, 34, 40] },
      showRate: { maxRate: 45, minRate: 20, ticks: [20, 26.3, 32.5, 38.8, 45] },
      winRate: { maxRate: 16, minRate: 8, ticks: [8, 10, 12, 14, 16] },
    }),
  ).toBe("勝率の色は8%から16%以上まで濃くなります");
  expect(
    formatWinRateHeatmapColorScaleAriaLabel(getVisibleWinRateHeatmapRateMetrics("all"), {
      quinellaRate: { maxRate: 40, minRate: 16, ticks: [16, 22, 28, 34, 40] },
      showRate: { maxRate: 45, minRate: 20, ticks: [20, 26.3, 32.5, 38.8, 45] },
      winRate: { maxRate: 16, minRate: 8, ticks: [8, 10, 12, 14, 16] },
    }),
  ).toBe("勝率は8%から16%以上、連対率は16%から40%以上、複勝率は20%から45%以上まで濃くなります");
});

it("collapses the color scale to one track when every visible metric shares a hue", () => {
  expect(
    getWinRateHeatmapColorScaleTracks([], {
      quinellaRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
      showRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
      winRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
    }),
  ).toStrictEqual([]);
  expect(
    getWinRateHeatmapColorScaleTracks(getVisibleWinRateHeatmapRateMetrics("winRate"), {
      quinellaRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
      showRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
      winRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
    }),
  ).toStrictEqual([
    { countKey: "winCount", hue: 8, key: "winRate", label: "勝率", shortLabel: "勝" },
  ]);
  expect(
    getWinRateHeatmapColorScaleTracks(getVisibleWinRateHeatmapRateMetrics("all"), {
      quinellaRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
      showRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
      winRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
    }),
  ).toStrictEqual([
    { countKey: "winCount", hue: 272, key: "winRate", label: "勝率", shortLabel: "勝" },
  ]);
  expect(
    getWinRateHeatmapColorScaleTracks(
      [
        { countKey: "winCount", hue: 8, key: "winRate", label: "勝率", shortLabel: "勝" },
        {
          countKey: "quinellaCount",
          hue: 36,
          key: "quinellaRate",
          label: "連対率",
          shortLabel: "連",
        },
      ],
      {
        quinellaRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
        showRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
        winRate: { maxRate: 40, minRate: 0, ticks: [0, 10, 20, 30, 40] },
      },
    ),
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

it("uses per-metric color scales for ばんえい venues", () => {
  expect(BAN_EI_WIN_RATE_HEATMAP_WIN_MAX_RATE).toBe(20);
  expect(BAN_EI_WIN_RATE_HEATMAP_QUINELLA_MAX_RATE).toBe(40);
  expect(BAN_EI_WIN_RATE_HEATMAP_SHOW_MAX_RATE).toBe(50);
  expect(BAN_EI_WIN_RATE_HEATMAP_WIN_TICKS).toStrictEqual([0, 5, 10, 15, 20]);
  expect(BAN_EI_WIN_RATE_HEATMAP_QUINELLA_TICKS).toStrictEqual([0, 10, 20, 30, 40]);
  expect(BAN_EI_WIN_RATE_HEATMAP_SHOW_TICKS).toStrictEqual([0, 12.5, 25, 37.5, 50]);
  expect(getWinRateHeatmapColorScale({ keibajoCode: "05", metricKey: "winRate" })).toStrictEqual({
    maxRate: 40,
    minRate: 0,
    ticks: [0, 10, 20, 30, 40],
  });
  expect(getWinRateHeatmapColorScale({ keibajoCode: "83", metricKey: "winRate" })).toStrictEqual({
    maxRate: 20,
    minRate: 0,
    ticks: [0, 5, 10, 15, 20],
  });
  expect(
    getWinRateHeatmapColorScale({ keibajoCode: "83", metricKey: "quinellaRate" }),
  ).toStrictEqual({
    maxRate: 40,
    minRate: 0,
    ticks: [0, 10, 20, 30, 40],
  });
  expect(getWinRateHeatmapColorScale({ keibajoCode: "83", metricKey: "showRate" })).toStrictEqual({
    maxRate: 50,
    minRate: 0,
    ticks: [0, 12.5, 25, 37.5, 50],
  });
  expect(
    resolveWinRateHeatmapColorScale({
      keibajoCode: "83",
      metricKey: "winRate",
      rates: [8, 16, 10],
    }),
  ).toStrictEqual({
    maxRate: 16,
    minRate: 8,
    ticks: [8, 10, 12, 14, 16],
  });
  expect(
    resolveWinRateHeatmapColorScale({
      keibajoCode: "83",
      metricKey: "winRate",
      rates: [12, 12],
    }),
  ).toStrictEqual({
    maxRate: 13,
    minRate: 11,
    ticks: [11, 11.5, 12, 12.5, 13],
  });
  expect(
    resolveWinRateHeatmapColorScale({
      keibajoCode: "05",
      metricKey: "winRate",
      rates: [8, 16],
    }),
  ).toStrictEqual({
    maxRate: 40,
    minRate: 0,
    ticks: [0, 10, 20, 30, 40],
  });
  expect(
    resolveWinRateHeatmapColorScale({
      keibajoCode: "83",
      metricKey: "winRate",
      rates: [],
    }),
  ).toStrictEqual({
    maxRate: 20,
    minRate: 0,
    ticks: [0, 5, 10, 15, 20],
  });
  expect(winRateHeatmapBackground({ hue: 8, maxRate: 12, minRate: 12, rate: 12 })).toBe(
    "hsl(8, 73%, 50%)",
  );
  expect(formatWinRateHeatmapColorScaleTick(12.5, 50)).toBe("12.5%");
  expect(formatWinRateHeatmapColorScaleTick(20, 20)).toBe("20%以上");
  expect(formatWinRateHeatmapColorScaleTick(50, 50)).toBe("50%以上");
  expect(
    getWinRateHeatmapColorScaleTracks(getVisibleWinRateHeatmapRateMetrics("all"), {
      quinellaRate: { maxRate: 30, minRate: 16, ticks: [16, 19.5, 23, 26.5, 30] },
      showRate: { maxRate: 45, minRate: 20, ticks: [20, 26.3, 32.5, 38.8, 45] },
      winRate: { maxRate: 16, minRate: 8, ticks: [8, 10, 12, 14, 16] },
    }),
  ).toStrictEqual([
    { countKey: "winCount", hue: 272, key: "winRate", label: "勝率", shortLabel: "勝" },
    {
      countKey: "quinellaCount",
      hue: 272,
      key: "quinellaRate",
      label: "連対率",
      shortLabel: "連",
    },
    { countKey: "showCount", hue: 272, key: "showRate", label: "複勝率", shortLabel: "複" },
  ]);
  expect(
    buildWinRateHeatmapColorScaleGradient({
      hue: 8,
      maxRate: 16,
      minRate: 8,
      ticks: [8, 10, 12, 14, 16],
    }),
  ).toBe(
    "linear-gradient(to right, hsl(8, 45%, 86%) 0%, hsl(8, 59%, 68%) 25%, hsl(8, 73%, 50%) 50%, hsl(8, 86%, 32%) 75%, hsl(8, 100%, 14%) 100%)",
  );
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

it("writes 4-digit start counts with parentheses on the combined heatmap graph", () => {
  const display = buildWinRateHeatmapDisplay({
    bloodlineRows: [],
    frameStats: [],
    horseResults: [],
    keibajoCode: "05",
    liveWeightKgByHorse: new Map(),
    runners: [runnerOne],
    showStarts: true,
    similarRows: [
      {
        category: "jockey",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 40,
        name: "Jockey A",
        quinellaCount: 20,
        quinellaRate: 25,
        showCount: 30,
        showRate: 37.5,
        starts: 1234,
        winCount: 16,
        winRate: 20,
      },
    ],
    viewMode: "all",
  });
  const jockeySwatch = display.rows[0]?.swatches.find(
    (swatch) => swatch.columnKey === "jockey" && swatch.metricKey === "winRate",
  );
  expect(jockeySwatch?.graphStartsLabel).toBe("(1234)");
  expect(jockeySwatch?.startsLabel).toBe("(1234)");
});

it("projects heatmap display labels with the same formatters the table uses", () => {
  const display = buildWinRateHeatmapDisplay({
    bloodlineRows: [],
    frameStats: [],
    horseResults: [],
    keibajoCode: "05",
    liveWeightKgByHorse: new Map(),
    runners: [runnerOne],
    showStarts: false,
    similarRows: [similarJockey],
    viewMode: "winRate",
  });
  const jockeySwatch = display.rows[0]?.swatches.find(
    (swatch) => swatch.columnKey === "jockey" && swatch.metricKey === "winRate",
  );
  expect(display.empty).toBe(false);
  expect(display.viewMode).toBe("winRate");
  expect(display.showCarriedWeight).toBe(true);
  expect(display.rows[0]?.horseNumber).toBe("1");
  expect(jockeySwatch?.valueLabel).toBe("20.0");
  expect(jockeySwatch?.startsLabel).toBe("(80)");
  expect(jockeySwatch?.graphStartsLabel).toBe(null);
  expect(jockeySwatch?.isZeroValue).toBe(false);
  expect(jockeySwatch?.isZeroGraphStarts).toBe(false);
});

it("includes (n) start labels on heatmap cells only when the レース数 flag is on", () => {
  const shown = buildWinRateHeatmapDisplay({
    bloodlineRows: [],
    frameStats: [],
    horseResults: [],
    keibajoCode: "05",
    liveWeightKgByHorse: new Map(),
    runners: [runnerOne],
    showStarts: true,
    similarRows: [similarJockey],
    viewMode: "winRate",
  });
  const jockeySwatch = shown.rows[0]?.swatches.find(
    (swatch) => swatch.columnKey === "jockey" && swatch.metricKey === "winRate",
  );
  expect(jockeySwatch?.startsLabel).toBe("(80)");
  expect(jockeySwatch?.graphStartsLabel).toBe("(80)");
  expect(jockeySwatch?.isZeroValue).toBe(false);
  expect(jockeySwatch?.isZeroGraphStarts).toBe(false);
});

it("marks zero rates and zero start counts so the graph text can stay faint", () => {
  const display = buildWinRateHeatmapDisplay({
    bloodlineRows: [],
    frameStats: [
      {
        averageFinish: 8,
        averagePopularity: 8,
        count: 10,
        details: [],
        frameNumber: "1",
        medianFinish: 8,
        medianPopularity: 8,
        quinellaCount: 0,
        quinellaRate: 0,
        runnerCount: 1,
        score: 0,
        showCount: 0,
        showRate: 0,
        winCount: 0,
        winRate: 0,
      },
    ],
    horseResults: [],
    keibajoCode: "05",
    liveWeightKgByHorse: new Map(),
    runners: [runnerOne],
    showStarts: true,
    similarRows: [],
    viewMode: "winRate",
  });
  const frameSwatch = display.rows[0]?.swatches.find(
    (swatch) => swatch.columnKey === "frame" && swatch.metricKey === "winRate",
  );
  expect(frameSwatch?.valueLabel).toBe("0.0");
  expect(frameSwatch?.graphStartsLabel).toBe("(10)");
  expect(frameSwatch?.isZeroValue).toBe(true);
  expect(frameSwatch?.isZeroGraphStarts).toBe(true);
  const hiddenStarts = buildWinRateHeatmapDisplay({
    bloodlineRows: [],
    frameStats: [
      {
        averageFinish: 8,
        averagePopularity: 8,
        count: 10,
        details: [],
        frameNumber: "1",
        medianFinish: 8,
        medianPopularity: 8,
        quinellaCount: 0,
        quinellaRate: 0,
        runnerCount: 1,
        score: 0,
        showCount: 0,
        showRate: 0,
        winCount: 0,
        winRate: 0,
      },
    ],
    horseResults: [],
    keibajoCode: "05",
    liveWeightKgByHorse: new Map(),
    runners: [runnerOne],
    showStarts: false,
    similarRows: [],
    viewMode: "winRate",
  });
  const hiddenFrameSwatch = hiddenStarts.rows[0]?.swatches.find(
    (swatch) => swatch.columnKey === "frame" && swatch.metricKey === "winRate",
  );
  expect(hiddenFrameSwatch?.startsLabel).toBe("(10)");
  expect(hiddenFrameSwatch?.graphStartsLabel).toBe(null);
  expect(hiddenFrameSwatch?.isZeroValue).toBe(true);
  expect(hiddenFrameSwatch?.isZeroGraphStarts).toBe(false);
});

it("marks a zero start count on the graph even when the rate is not zero", () => {
  const display = buildWinRateHeatmapDisplay({
    bloodlineRows: [],
    frameStats: [],
    horseResults: [],
    keibajoCode: "05",
    liveWeightKgByHorse: new Map(),
    runners: [runnerOne],
    showStarts: true,
    similarRows: [
      {
        category: "jockey",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 0,
        name: "Jockey A",
        quinellaCount: 0,
        quinellaRate: 12.5,
        showCount: 0,
        showRate: 12.5,
        starts: 0,
        winCount: 0,
        winRate: 12.5,
      },
    ],
    viewMode: "winRate",
  });
  const jockeySwatch = display.rows[0]?.swatches.find(
    (swatch) => swatch.columnKey === "jockey" && swatch.metricKey === "winRate",
  );
  expect(jockeySwatch?.valueLabel).toBe("12.5");
  expect(jockeySwatch?.graphStartsLabel).toBe("(0)");
  expect(jockeySwatch?.isZeroValue).toBe(false);
  expect(jockeySwatch?.isZeroGraphStarts).toBe(true);
});
