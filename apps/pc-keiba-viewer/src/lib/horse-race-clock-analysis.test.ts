// This file runs with bun.

import { expect, it } from "vitest";

import {
  buildClockAnalysisRows,
  buildScaledWinnerPar,
  buildScatterView,
  clockParFromRaceTimeStats,
  clockParToRaceTimeFields,
  formatClockPointTooltip,
  formatClockReferenceTooltip,
  withScaledWinnerClockStats,
} from "./horse-race-clock-analysis";
import type { HorseRaceResult, RaceTimeStats, RaceTimeTargetRace } from "./race-types";

const result = (overrides: Partial<HorseRaceResult>): HorseRaceResult => ({
  babajotaiCodeDirt: "1",
  babajotaiCodeShiba: "0",
  banushimei: "馬主",
  barei: "04",
  bataiju: "480",
  bamei: "テストホース",
  chokyoshimeiRyakusho: "調教師",
  currentBarei: "04",
  currentJockey: "騎手",
  currentSeibetsuCode: "1",
  currentUmaban: "01",
  corner1: "03",
  corner2: "04",
  corner3: "05",
  corner4: "06",
  futanJuryo: "550",
  gradeCode: "00",
  hassoJikoku: "1200",
  jockeyName: "騎手",
  juryoShubetsuCode: "1",
  kakuteiChakujun: "01",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0322",
  keibajoCode: "05",
  kettoTorokuBango: "2022100001",
  kishumeiRyakusho: "騎手",
  kohan3f: "351",
  kyori: "1700",
  kyosoJokenCode: "005",
  kyosoJokenMeisho: "3歳",
  kyosoKigoCode: "000",
  kyosoShubetsuCode: "12",
  kyosomeiFukudai: null,
  kyosomeiHondai: "テストレース",
  kyosomeiKakkonai: null,
  raceBango: "01",
  seibetsuCode: "1",
  sohaTime: "1050",
  tanshoNinkijun: "03",
  tanshoOdds: "45",
  tenkoCode: "1",
  timeSa: "002",
  trackCode: "24",
  umaban: "01",
  wakuban: "1",
  zogenFugo: null,
  zogenSa: null,
  ...overrides,
});

const targetRace = (overrides: Partial<RaceTimeTargetRace>): RaceTimeTargetRace => ({
  date: "20260301",
  horseName: "1着馬",
  horseNumber: "01",
  jockeyName: "騎手",
  keibajoCode: "43",
  kohan3f: "357",
  kyori: "1700",
  ownerName: "馬主",
  popularity: "01",
  raceName: "一般",
  raceNumber: "11",
  raceTime: "1050",
  trainerName: "調教師",
  ...overrides,
});

const stats = (): RaceTimeStats => ({
  averageKohan3f: 360,
  averageRaceTime: 575,
  correlationRows: [],
  fastestDetail: null,
  fastestKohan3f: 340,
  fastestRaceTime: 575,
  medianKohan3f: 355,
  medianRaceTime: 575,
  raceCount: 2,
  targetRaces: [
    targetRace({ kyori: "1000", raceTime: "575" }),
    targetRace({ date: "20260308", kyori: "1700", raceTime: "1050" }),
  ],
});

it("scales mixed-distance winner clocks onto the current race distance", () => {
  const par = buildScaledWinnerPar({
    currentDistance: "1700",
    races: [
      targetRace({ kyori: "1000", raceTime: "575" }),
      targetRace({ date: "20260308", kyori: "1700", raceTime: "1050" }),
    ],
  });
  expect(par.fastestRaceTime).toBe(977.5);
  expect(par.averageRaceTime).toBe(1013.75);
  expect(par.medianRaceTime).toBe(1013.75);
  expect(par.fastestKohan3f).toBe(357);
  expect(par.sampleCount).toBe(2);
});

it("drops winner clocks that have no distance when the current race has a distance", () => {
  expect(
    buildScaledWinnerPar({
      currentDistance: "1700",
      races: [targetRace({ kyori: "", raceTime: "575" })],
    }).sampleCount,
  ).toBe(0);
  expect(
    buildScaledWinnerPar({
      currentDistance: "1700",
      races: [{ ...targetRace({}), kyori: 1000 }],
    }).sampleCount,
  ).toBe(0);
});

it("keeps raw winner clocks when the current race has no distance", () => {
  expect(
    buildScaledWinnerPar({
      currentDistance: "",
      races: [targetRace({ kyori: "1000", raceTime: "575" })],
    }).fastestRaceTime,
  ).toBe(575);
});

it("skips unparseable winner race times", () => {
  expect(
    buildScaledWinnerPar({
      currentDistance: "1700",
      races: [targetRace({ raceTime: "0000" })],
    }).sampleCount,
  ).toBe(0);
});

it("replaces unscaled catalog fastest times with scaled winner clocks", () => {
  const scaled = withScaledWinnerClockStats(stats(), "1700");
  expect(scaled.fastestRaceTime).toBe(977.5);
  expect(scaled.averageRaceTime).toBe(1013.75);
  expect(scaled.raceCount).toBe(2);
  expect(
    clockParToRaceTimeFields(buildScaledWinnerPar({ currentDistance: "1700", races: [] })),
  ).toStrictEqual({
    averageKohan3f: null,
    averageRaceTime: null,
    fastestKohan3f: null,
    fastestRaceTime: null,
    medianKohan3f: null,
    medianRaceTime: null,
  });
});

it("keeps original stats when no winner clock can be scaled", () => {
  expect(
    withScaledWinnerClockStats(
      {
        averageKohan3f: 360,
        averageRaceTime: 575,
        correlationRows: [],
        fastestDetail: null,
        fastestKohan3f: 340,
        fastestRaceTime: 575,
        medianKohan3f: 355,
        medianRaceTime: 575,
        raceCount: 1,
        targetRaces: [],
      },
      "1700",
    ).fastestRaceTime,
  ).toBe(575);
});

it("scales a horse clock to the current distance and skips incomplete rows", () => {
  const rows = buildClockAnalysisRows({
    currentDistance: "1700",
    results: [
      result({ kyori: "1700", sohaTime: "1050" }),
      result({
        bamei: "短い距離",
        currentUmaban: "02",
        kyori: "1000",
        sohaTime: "575",
        umaban: "02",
      }),
      result({ kohan3f: "000", sohaTime: "1050" }),
      result({ kyori: "", sohaTime: "1050" }),
      result({ kaisaiTsukihi: "00", sohaTime: "1050" }),
    ],
    runners: [],
  });
  expect(rows.length).toBe(2);
  expect(rows[0]?.scaledTimeTenths).toBe(1050);
  expect(rows[1]?.scaledTimeTenths).toBe(977.5);
});

it("uses unscaled horse clocks when the current race has no distance", () => {
  const rows = buildClockAnalysisRows({
    currentDistance: null,
    results: [result({ kyori: "1000", sohaTime: "575" })],
    runners: [],
  });
  expect(rows[0]?.scaledTimeTenths).toBe(575);
});

it("plots every filtered race with scaled winner references, not a short mixed-distance raw clock", () => {
  const rows = buildClockAnalysisRows({
    currentDistance: "1700",
    results: [
      result({ kaisaiTsukihi: "0322", sohaTime: "1050" }),
      result({ kaisaiTsukihi: "0101", sohaTime: "1100" }),
      result({
        bamei: "2着馬",
        currentUmaban: "02",
        kaisaiTsukihi: "0322",
        sohaTime: "1060",
        umaban: "02",
      }),
    ],
    runners: [],
  });
  const view = buildScatterView(
    rows,
    buildScaledWinnerPar({
      currentDistance: "1700",
      races: [
        targetRace({ kyori: "1000", raceTime: "575" }),
        targetRace({ kyori: "1700", raceTime: "1050" }),
      ],
    }),
  );
  expect(view === null).toBe(false);
  if (view === null) {
    throw new Error("expected scatter view");
  }
  expect(view.title).toBe("換算タイム×上がり3F");
  expect(view.points.length).toBe(3);
  expect(view.points[0]?.timeLabel).toBe("1:50.0");
  expect(view.points[1]?.timeLabel).toBe("1:45.0");
  expect(view.points[2]?.timeLabel).toBe("1:46.0");
  expect(view.references.map((line) => line.kind)).toStrictEqual([
    "fastestTime",
    "averageTime",
    "medianTime",
    "fastestKohan",
    "averageKohan",
    "medianKohan",
  ]);
  expect(view.references[0]?.label).toBe("最速レースタイム 1:37.8");
  expect(view.references[1]?.label).toBe("平均レースタイム 1:41.4");
  expect(view.references[2]?.label).toBe("中央値レースタイム 1:41.4");
  expect(view.references[3]?.label).toBe("最速上がり3F 35.7");
  expect(view.references[4]?.label).toBe("平均上がり3F 35.7");
  expect(view.references[5]?.label).toBe("中央値上がり3F 35.7");
  expect(view.references[0]?.stroke).toBe("#be123c");
  expect(view.references[1]?.stroke).toBe("#166534");
  expect(view.references[2]?.stroke).toBe("#4338ca");
  expect(view.references[0]?.strokeDasharray).toBe("6 4");
  expect(view.references[3]?.stroke).toBe("#be123c");
  expect(view.references[3]?.strokeDasharray).toBe("2 3");
  expect(view.references[0]?.y1).toBe(view.references[0]?.y2);
  expect(view.references[3]?.x1).toBe(view.references[3]?.x2);
});

it("returns no scatter when there are no plottable rows", () => {
  expect(buildScatterView([], buildScaledWinnerPar({ currentDistance: "1700", races: [] }))).toBe(
    null,
  );
});

it("keeps race times when winner last-3F is missing", () => {
  const par = buildScaledWinnerPar({
    currentDistance: "1700",
    races: [targetRace({ kohan3f: "000", kyori: "1700", raceTime: "1050" })],
  });
  expect(par.fastestRaceTime).toBe(1050);
  expect(par.fastestKohan3f).toBe(null);
  expect(par.averageKohan3f).toBe(null);
  expect(par.medianKohan3f).toBe(null);
});

it("reads same-condition lines from race-time stats without target-race kyori", () => {
  const par = clockParFromRaceTimeStats({
    averageKohan3f: 392,
    averageRaceTime: 1490,
    correlationRows: [],
    fastestDetail: null,
    fastestKohan3f: 356,
    fastestRaceTime: 1436,
    medianKohan3f: 391.5,
    medianRaceTime: 1489,
    raceCount: 400,
    targetRaces: [],
  });
  const view = buildScatterView(
    buildClockAnalysisRows({
      currentDistance: "1700",
      results: [result({})],
      runners: [],
    }),
    par,
  );
  expect(view === null).toBe(false);
  if (view === null) {
    throw new Error("expected scatter view from stats par");
  }
  expect(view.references.map((line) => line.label)).toStrictEqual([
    "最速レースタイム 2:23.6",
    "平均レースタイム 2:29.0",
    "中央値レースタイム 2:28.9",
    "最速上がり3F 35.6",
    "平均上がり3F 39.2",
    "中央値上がり3F 39.1",
  ]);
  expect(view.references.map((line) => line.stroke)).toStrictEqual([
    "#be123c",
    "#166534",
    "#4338ca",
    "#be123c",
    "#166534",
    "#4338ca",
  ]);
  expect(view.references.map((line) => line.strokeDasharray)).toStrictEqual([
    "6 4",
    "6 4",
    "6 4",
    "2 3",
    "2 3",
    "2 3",
  ]);
});

it("returns an empty par when race-time stats are missing", () => {
  expect(clockParFromRaceTimeStats(null)).toStrictEqual({
    averageKohan3f: null,
    averageRaceTime: null,
    fastestKohan3f: null,
    fastestRaceTime: null,
    medianKohan3f: null,
    medianRaceTime: null,
    sampleCount: 0,
  });
});

it("counts no sample when every race-time stats clock field is empty", () => {
  expect(
    clockParFromRaceTimeStats({
      averageKohan3f: null,
      averageRaceTime: null,
      correlationRows: [],
      fastestDetail: null,
      fastestKohan3f: null,
      fastestRaceTime: null,
      medianKohan3f: null,
      medianRaceTime: null,
      raceCount: 400,
      targetRaces: [],
    }),
  ).toStrictEqual({
    averageKohan3f: null,
    averageRaceTime: null,
    fastestKohan3f: null,
    fastestRaceTime: null,
    medianKohan3f: null,
    medianRaceTime: null,
    sampleCount: 0,
  });
});

it("plots scatter without same-condition lines when winner clocks are missing", () => {
  const view = buildScatterView(
    buildClockAnalysisRows({ currentDistance: "1700", results: [result({})], runners: [] }),
    buildScaledWinnerPar({ currentDistance: "1700", races: [] }),
  );
  expect(view === null).toBe(false);
  if (view === null) {
    throw new Error("expected scatter view");
  }
  expect(view.references).toStrictEqual([]);
});

it("formats a clock tooltip with horse, date, and both clocks", () => {
  expect(
    formatClockPointTooltip({
      dateLabel: "2026-03-22",
      fill: "#d71920",
      horseName: "テストホース",
      id: "1-1-1050",
      kohanLabel: "35.1",
      r: 5,
      stroke: "#d71920",
      timeLabel: "1:45.0",
      umaban: "1",
      x: 10,
      y: 20,
    }),
  ).toStrictEqual(["1 テストホース", "日付 2026-03-22", "換算タイム 1:45.0", "上がり3F 35.1"]);
});

it("formats a same-condition reference-line tooltip", () => {
  expect(
    formatClockReferenceTooltip({
      kind: "fastestTime",
      label: "最速レースタイム 2:23.6",
      stroke: "#be123c",
      strokeDasharray: "6 4",
      x1: 72,
      x2: 700,
      y1: 120,
      y2: 120,
    }),
  ).toStrictEqual(["最速レースタイム 2:23.6"]);
});

it("paints scatter points with JRA frame colors and a dark outline on white", () => {
  const white = buildClockAnalysisRows({
    currentDistance: "1700",
    results: [result({ wakuban: "1" })],
    runners: [],
  });
  const red = buildClockAnalysisRows({
    currentDistance: "1700",
    results: [result({ wakuban: "3" })],
    runners: [],
  });
  const unknown = buildClockAnalysisRows({
    currentDistance: "1700",
    results: [result({ wakuban: "" })],
    runners: [],
  });
  expect(white[0]?.fill).toBe("#ffffff");
  expect(white[0]?.stroke).toBe("#111111");
  expect(red[0]?.fill).toBe("#d71920");
  expect(red[0]?.stroke).toBe("#d71920");
  expect(unknown[0]?.fill).toBe("#52525b");
});

it("prefers the current-race wakuban from the matching runner", () => {
  const rows = buildClockAnalysisRows({
    currentDistance: "1700",
    results: [result({ kettoTorokuBango: "2022100001", wakuban: "1" })],
    runners: [{ kettoTorokuBango: "2022100001", wakuban: "8" }],
  });
  expect(rows[0]?.fill).toBe("#f4a3c4");
  expect(rows[0]?.stroke).toBe("#f4a3c4");
});

it("keeps the result wakuban when the runner map has no usable ketto or frame", () => {
  const rows = buildClockAnalysisRows({
    currentDistance: "1700",
    results: [result({ kettoTorokuBango: "2022100001", wakuban: "1" })],
    runners: [
      { kettoTorokuBango: "", wakuban: "4" },
      { kettoTorokuBango: "2022100001", wakuban: "" },
    ],
  });
  expect(rows[0]?.fill).toBe("#ffffff");
  expect(rows[0]?.stroke).toBe("#111111");
});
