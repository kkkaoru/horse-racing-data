// This file runs with bun.

import { expect, it } from "vitest";

import {
  buildDrawnBanEiAbilityChart,
  formatBanEiFinishMarkLabel,
  buildDrawnRaceTimeChart,
  DEFAULT_SHOW_RESULTS_CHART,
  banEiFinishMarkRadius,
  formatCarriedWeightDeltaLabel,
  formatCarriedWeightKgLabel,
  formatFinishRankAxisLabel,
  formatKohan3fTenthsLabel,
  formatRaceTimeChartTooltip,
  formatRaceTimeTenthsLabel,
  parseBanEiCarriedWeightKg,
  parseKohan3fTenths,
  parseRaceDistanceMeters,
  parseRaceFinishRank,
  parseSohaTimeTenths,
  raceTimeChartEmptyMessage,
  raceTimeChartNote,
  raceTimeDistanceWeight,
  scaleSohaTimeToDistance,
  RACE_TIME_CHART_BAN_EI_EMPTY,
  RACE_TIME_CHART_BAN_EI_NOTE,
  RACE_TIME_CHART_BAN_EI_WEIGHT_X_AXIS_TITLE,
  RACE_TIME_CHART_BAN_EI_X_AXIS_TITLE,
  RACE_TIME_CHART_EMPTY,
  RACE_TIME_CHART_NOTE,
  RACE_TIME_CHART_X_AXIS_TITLE,
  RACE_TIME_CHART_Y_AXIS_TITLE,
  raceTimeFinishStroke,
  scheduledWeightMarkPoints,
} from "./horse-race-time-charts";
import type { HorseRaceResult, RaceTimeStats, Runner } from "./race-types";

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
  kyori: "1800",
  kyosoJokenCode: "005",
  kyosoJokenMeisho: "3歳",
  kyosoKigoCode: "000",
  kyosoShubetsuCode: "12",
  kyosomeiFukudai: null,
  kyosomeiHondai: "テストレース",
  kyosomeiKakkonai: null,
  raceBango: "01",
  seibetsuCode: "1",
  sohaTime: "1100",
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

const runner = (overrides: Partial<Runner>): Runner => ({
  bamei: "出走馬",
  banushimei: "馬主",
  barei: "04",
  bataiju: "480",
  chokyoshimeiRyakusho: "調教師",
  damSireName: null,
  futanJuryo: "262",
  kakuteiChakujun: null,
  kettoTorokuBango: "2022100001",
  kishumeiRyakusho: "騎手",
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  kohan3f: null,
  seibetsuCode: "1",
  sireName: null,
  sireSireName: null,
  sohaTime: null,
  tanshoNinkijun: null,
  tanshoOdds: null,
  timeSa: null,
  umaban: "01",
  wakuban: "1",
  zogenFugo: null,
  zogenSa: null,
  ...overrides,
});

const stats = (overrides: Partial<RaceTimeStats> = {}): RaceTimeStats => ({
  averageKohan3f: 360,
  averageRaceTime: 1120,
  correlationRows: [],
  fastestDetail: null,
  fastestKohan3f: 340,
  fastestRaceTime: 1050,
  medianKohan3f: 355,
  medianRaceTime: 1080,
  raceCount: 10,
  targetRaces: [],
  ...overrides,
});

it("defaults the results view to the chart", () => {
  expect(DEFAULT_SHOW_RESULTS_CHART).toBe(true);
  expect(RACE_TIME_CHART_NOTE).toBe(
    "各点は出走予定馬の過去レースです。レースタイムは今走の距離に比例換算しています。今走と同じ距離ほど点は濃く、距離が離れるほど薄くします。上ほど換算タイムが速く、右ほど上がり3Fが速い。点の色は着順、数字は馬番。最速・平均・中央値の線は目安です。",
  );
  expect(RACE_TIME_CHART_X_AXIS_TITLE).toBe("上がり3F（右が速い）");
  expect(RACE_TIME_CHART_Y_AXIS_TITLE).toBe("換算レースタイム（今走距離、上が速い）");
  expect(RACE_TIME_CHART_EMPTY).toBe("レースタイムと上がり3Fが揃った競走成績がありません。");
  expect(raceTimeChartNote(false)).toBe(
    "各点は出走予定馬の過去レースです。レースタイムは今走の距離に比例換算しています。今走と同じ距離ほど点は濃く、距離が離れるほど薄くします。上ほど換算タイムが速く、右ほど上がり3Fが速い。点の色は着順、数字は馬番。最速・平均・中央値の線は目安です。",
  );
  expect(raceTimeChartEmptyMessage(false)).toBe(
    "レースタイムと上がり3Fが揃った競走成績がありません。",
  );
});

it("uses finish rank instead of last 3F copy for Ban-ei charts", () => {
  expect(RACE_TIME_CHART_BAN_EI_X_AXIS_TITLE).toBe("着順（右が上位）");
  expect(RACE_TIME_CHART_BAN_EI_WEIGHT_X_AXIS_TITLE).toBe("斤量（右が重い）");
  expect(RACE_TIME_CHART_BAN_EI_EMPTY).toBe(
    "レースタイムと着順と斤量が揃った競走成績がありません。",
  );
  expect(RACE_TIME_CHART_BAN_EI_NOTE).toBe(
    "ばんえいには上がり3Fがありません。1つの図で斤量・換算タイム・着順を見ます。上ほど速く、右ほど斤量が重い。点の中の数字と色・大きさが着順、右の数字は馬番。◇は今走の予定斤量、横線は過去斤量との差。同じ馬の複数レースは薄い線でつなぎます。",
  );
  expect(raceTimeChartNote(true)).toBe(
    "ばんえいには上がり3Fがありません。1つの図で斤量・換算タイム・着順を見ます。上ほど速く、右ほど斤量が重い。点の中の数字と色・大きさが着順、右の数字は馬番。◇は今走の予定斤量、横線は過去斤量との差。同じ馬の複数レースは薄い線でつなぎます。",
  );
  expect(raceTimeChartEmptyMessage(true)).toBe(
    "レースタイムと着順と斤量が揃った競走成績がありません。",
  );
  expect(formatFinishRankAxisLabel(1)).toBe("1着");
  expect(formatFinishRankAxisLabel(8.4)).toBe("8着");
  expect(formatCarriedWeightKgLabel(610)).toBe("610kg");
  expect(formatCarriedWeightDeltaLabel(0)).toBe("±0kg");
  expect(formatCarriedWeightDeltaLabel(10.4)).toBe("+10kg");
  expect(formatCarriedWeightDeltaLabel(-10.4)).toBe("-10kg");
  expect(scheduledWeightMarkPoints(10, 20)).toBe("10,14 16,20 10,26 4,20");
  expect(parseBanEiCarriedWeightKg("262")).toBe(610);
  expect(parseBanEiCarriedWeightKg("26C")).toBe(620);
  expect(parseBanEiCarriedWeightKg("FFF")).toBe(null);
  expect(parseBanEiCarriedWeightKg("000")).toBe(null);
  expect(formatBanEiFinishMarkLabel(1)).toBe("1");
  expect(formatBanEiFinishMarkLabel(8)).toBe("8");
  expect(formatBanEiFinishMarkLabel(null)).toBe("-");
  expect(banEiFinishMarkRadius(1)).toBe(10);
  expect(banEiFinishMarkRadius(2)).toBe(8.6);
  expect(banEiFinishMarkRadius(3)).toBe(7.6);
  expect(banEiFinishMarkRadius(4)).toBe(6.6);
  expect(banEiFinishMarkRadius(8)).toBe(5.6);
  expect(banEiFinishMarkRadius(null)).toBe(5.6);
});

it("parses soha times as tenths and packed ban-ei clocks", () => {
  expect(parseSohaTimeTenths("1100", false)).toBe(1100);
  expect(parseSohaTimeTenths("3188", true)).toBe(1988);
  expect(parseSohaTimeTenths("0000", false)).toBe(null);
  expect(parseSohaTimeTenths(null, false)).toBe(null);
});

it("parses last-3F tenths and finish ranks", () => {
  expect(parseKohan3fTenths("351")).toBe(351);
  expect(parseKohan3fTenths("000")).toBe(null);
  expect(parseRaceFinishRank("01")).toBe(1);
  expect(parseRaceFinishRank("00")).toBe(null);
  expect(parseRaceFinishRank("abc")).toBe(null);
  expect(parseRaceDistanceMeters("1800")).toBe(1800);
  expect(parseRaceDistanceMeters("0")).toBe(null);
  expect(scaleSohaTimeToDistance(1000, 1600, 1800)).toBe(1125);
  expect(raceTimeDistanceWeight(1800, 1800)).toBe(1);
  expect(raceTimeDistanceWeight(1600, 1800)).toBe(0.5);
  expect(raceTimeDistanceWeight(1000, 1800)).toBe(0.18);
  expect(raceTimeDistanceWeight(null, 1800)).toBe(1);
  expect(raceTimeDistanceWeight(1600, null)).toBe(1);
});

it("formats race times with and without minutes", () => {
  expect(formatRaceTimeTenthsLabel(1100)).toBe("1:50.0");
  expect(formatRaceTimeTenthsLabel(351)).toBe("35.1");
  expect(formatKohan3fTenthsLabel(351)).toBe("35.1");
});

it("colors finish ranks from first through unplaced", () => {
  expect(raceTimeFinishStroke(1)).toBe("#b45309");
  expect(raceTimeFinishStroke(2)).toBe("#64748b");
  expect(raceTimeFinishStroke(3)).toBe("#c2410c");
  expect(raceTimeFinishStroke(5)).toBe("#355f9f");
  expect(raceTimeFinishStroke(6)).toBe("#94a3b8");
  expect(raceTimeFinishStroke(null)).toBe("#94a3b8");
});

it("returns null when no race has both a clock and a last 3F", () => {
  expect(buildDrawnRaceTimeChart({ currentDistance: "1800", results: [], stats: null })).toBe(null);
  expect(
    buildDrawnRaceTimeChart({
      currentDistance: "1800",
      results: [result({ kohan3f: "000", sohaTime: "1100" })],
      stats: null,
    }),
  ).toBe(null);
  expect(
    buildDrawnRaceTimeChart({
      currentDistance: "1800",
      results: [result({ kohan3f: "351", sohaTime: "0000" })],
      stats: null,
    }),
  ).toBe(null);
});

it("orders two races of the same horse by race identity", () => {
  const drawn = buildDrawnRaceTimeChart({
    currentDistance: "1800",
    results: [
      result({ kaisaiTsukihi: "0401", kyosomeiHondai: "後のレース" }),
      result({ kaisaiTsukihi: "0322", kyosomeiHondai: "先のレース" }),
    ],
    stats: null,
  });
  if (drawn === null) {
    throw new Error("expected same-horse points");
  }
  expect(drawn.points.length).toBe(2);
  expect(drawn.points[0]?.id).toBe("1-20260322-05-01");
  expect(drawn.points[1]?.id).toBe("1-20260401-05-01");
});

it("plots a faster last 3F to the right and a faster race time higher", () => {
  const drawn = buildDrawnRaceTimeChart({
    currentDistance: "1800",
    results: [
      result({
        bamei: "遅い馬",
        currentUmaban: "02",
        kakuteiChakujun: "08",
        kohan3f: "400",
        sohaTime: "1200",
        umaban: "02",
      }),
      result({
        bamei: "速い馬",
        currentUmaban: "01",
        kakuteiChakujun: "01",
        kohan3f: "330",
        sohaTime: "1000",
      }),
    ],
    stats: null,
  });
  if (drawn === null) {
    throw new Error("expected a race-time scatter");
  }
  expect(drawn.points.length).toBe(2);
  expect(drawn.points[0]?.horseName).toBe("速い馬");
  expect(drawn.points[1]?.horseName).toBe("遅い馬");
  const fast = drawn.points[0];
  const slow = drawn.points[1];
  if (fast === undefined || slow === undefined) {
    throw new Error("expected fast and slow points");
  }
  expect(fast.x > slow.x).toBe(true);
  expect(fast.y < slow.y).toBe(true);
  expect(drawn.references.length).toBe(0);
});

it("draws fastest average and median reference lines for race time and last 3F", () => {
  const drawn = buildDrawnRaceTimeChart({
    currentDistance: "1800",
    results: [result({})],
    stats: stats(),
  });
  if (drawn === null) {
    throw new Error("expected reference lines");
  }
  expect(drawn.references.map((line) => line.kind)).toStrictEqual([
    "fastestRaceTime",
    "averageRaceTime",
    "medianRaceTime",
    "fastestKohan3f",
    "averageKohan3f",
    "medianKohan3f",
  ]);
  expect(drawn.references[0]?.orientation).toBe("horizontal");
  expect(drawn.references[3]?.orientation).toBe("vertical");
  expect(drawn.references[0]?.y1 === drawn.references[0]?.y2).toBe(true);
  expect(drawn.references[3]?.x1 === drawn.references[3]?.x2).toBe(true);
});

it("skips missing reference stats", () => {
  const drawn = buildDrawnRaceTimeChart({
    currentDistance: "1800",
    results: [result({})],
    stats: stats({
      averageKohan3f: null,
      averageRaceTime: null,
      fastestKohan3f: null,
      fastestRaceTime: 1050,
      medianKohan3f: null,
      medianRaceTime: null,
    }),
  });
  if (drawn === null) {
    throw new Error("expected a partial reference set");
  }
  expect(drawn.references.length).toBe(1);
  expect(drawn.references[0]?.kind).toBe("fastestRaceTime");
});

it("formats a tooltip with horse, date, venue, finish, and clocks", () => {
  const drawn = buildDrawnRaceTimeChart({
    currentDistance: "1800",
    results: [result({ kaisaiNen: "2025", kaisaiTsukihi: "1012" })],
    stats: null,
  });
  if (drawn === null || drawn.points[0] === undefined) {
    throw new Error("expected a tooltip point");
  }
  expect(formatRaceTimeChartTooltip(drawn.points[0])).toStrictEqual([
    "1 テストホース",
    "2025-10-12",
    "東京",
    "過去騎手 騎手",
    "予定騎手 騎手",
    "距離 1800m",
    "着順 1",
    "レースタイム 1:50.0",
    "上がり3F 35.1",
  ]);
});

it("uses a dash when the race date is incomplete", () => {
  const drawn = buildDrawnRaceTimeChart({
    currentDistance: "1800",
    results: [result({ kaisaiNen: "20", kaisaiTsukihi: "10" })],
    stats: null,
  });
  if (drawn === null || drawn.points[0] === undefined) {
    throw new Error("expected an undated point");
  }
  expect(drawn.points[0].dateLabel).toBe("-");
  expect(formatRaceTimeChartTooltip(drawn.points[0])[6]).toBe("着順 1");
});

it("shows the past rider and the scheduled rider in the tooltip", () => {
  const drawn = buildDrawnRaceTimeChart({
    currentDistance: "1800",
    results: [
      result({
        currentJockey: "川田将雅",
        kishumeiRyakusho: "武豊",
      }),
    ],
    stats: null,
  });
  if (drawn === null || drawn.points[0] === undefined) {
    throw new Error("expected a jockey tooltip point");
  }
  expect(drawn.points[0].pastJockeyLabel).toBe("武豊");
  expect(drawn.points[0].scheduledJockeyLabel).toBe("川田将雅");
  expect(formatRaceTimeChartTooltip(drawn.points[0])[3]).toBe("過去騎手 武豊");
  expect(formatRaceTimeChartTooltip(drawn.points[0])[4]).toBe("予定騎手 川田将雅");
});

it("uses a dash when either jockey name is missing", () => {
  const drawn = buildDrawnRaceTimeChart({
    currentDistance: "1800",
    results: [result({ currentJockey: null, kishumeiRyakusho: "" })],
    stats: null,
  });
  if (drawn === null || drawn.points[0] === undefined) {
    throw new Error("expected a missing-jockey tooltip point");
  }
  expect(formatRaceTimeChartTooltip(drawn.points[0])[3]).toBe("過去騎手 -");
  expect(formatRaceTimeChartTooltip(drawn.points[0])[4]).toBe("予定騎手 -");
});

it("uses a dash in the tooltip when finish is missing", () => {
  const drawn = buildDrawnRaceTimeChart({
    currentDistance: "1800",
    results: [result({ kakuteiChakujun: "00" })],
    stats: null,
  });
  if (drawn === null || drawn.points[0] === undefined) {
    throw new Error("expected an unplaced point");
  }
  expect(formatRaceTimeChartTooltip(drawn.points[0])[6]).toBe("着順 -");
});

it("scales different distances onto the current race distance", () => {
  const drawn = buildDrawnRaceTimeChart({
    currentDistance: "1800",
    results: [
      result({
        bamei: "1600馬",
        currentUmaban: "01",
        kyori: "1600",
        sohaTime: "1000",
      }),
      result({
        bamei: "2000馬",
        currentUmaban: "02",
        kyori: "2000",
        sohaTime: "1250",
        umaban: "02",
      }),
    ],
    stats: null,
  });
  if (drawn === null) {
    throw new Error("expected distance-scaled points");
  }
  const short = drawn.points.find((point) => point.horseName === "1600馬");
  const long = drawn.points.find((point) => point.horseName === "2000馬");
  if (short === undefined || long === undefined) {
    throw new Error("expected both distance points");
  }
  expect(short.scaledSohaTimeTenths).toBe(1125);
  expect(long.scaledSohaTimeTenths).toBe(1125);
  expect(short.y).toBe(long.y);
  expect(short.distanceWeight).toBe(0.5);
  expect(long.distanceWeight).toBe(0.5);
  expect(short.distanceDeltaMeters).toBe(-200);
  expect(long.distanceDeltaMeters).toBe(200);
  expect(formatRaceTimeChartTooltip(short)).toStrictEqual([
    "1 1600馬",
    "2026-03-22",
    "東京",
    "過去騎手 騎手",
    "予定騎手 騎手",
    "距離 1600m",
    "距離差 -200m",
    "着順 1",
    "レースタイム 1:40.0",
    "換算 1:52.5",
    "上がり3F 35.1",
  ]);
  expect(formatRaceTimeChartTooltip(long)).toStrictEqual([
    "2 2000馬",
    "2026-03-22",
    "東京",
    "過去騎手 騎手",
    "予定騎手 騎手",
    "距離 2000m",
    "距離差 +200m",
    "着順 1",
    "レースタイム 2:05.0",
    "換算 1:52.5",
    "上がり3F 35.1",
  ]);
});

it("drops a result with no distance when the current race distance is known", () => {
  const drawn = buildDrawnRaceTimeChart({
    currentDistance: "1800",
    results: [
      result({ kyori: "0000", sohaTime: "1100" }),
      result({ bamei: "距離あり", sohaTime: "1100" }),
    ],
    stats: null,
  });
  if (drawn === null) {
    throw new Error("expected the distance-known point");
  }
  expect(drawn.points.length).toBe(1);
  expect(drawn.points[0]?.horseName).toBe("距離あり");
});

it("keeps raw clocks when the current race distance is missing", () => {
  const drawn = buildDrawnRaceTimeChart({
    currentDistance: "",
    results: [
      result({ kyori: "1600", sohaTime: "1000" }),
      result({
        bamei: "距離なし",
        kyori: null,
        sohaTime: "1000",
        umaban: "02",
        currentUmaban: "02",
      }),
    ],
    stats: null,
  });
  if (drawn === null) {
    throw new Error("expected raw-clock points");
  }
  expect(drawn.points.length).toBe(2);
  expect(drawn.points[0]?.scaledSohaTimeTenths).toBe(1000);
  expect(drawn.points[1]?.scaledSohaTimeTenths).toBe(1000);
  expect(drawn.points[0]?.distanceWeight).toBe(1);
  expect(drawn.points[0]?.distanceDeltaMeters).toBe(null);
  expect(drawn.points[1]?.distanceDeltaMeters).toBe(null);
});

it("plots Ban-ei clocks without last 3F and puts a better finish to the right", () => {
  const drawn = buildDrawnBanEiAbilityChart({
    currentDistance: "200",
    keibajoCode: "83",
    results: [
      result({
        bamei: "1着馬",
        futanJuryo: "262",
        kakuteiChakujun: "01",
        keibajoCode: "83",
        kohan3f: "000",
        kyori: "200",
        sohaTime: "3188",
      }),
      result({
        bamei: "8着馬",
        currentUmaban: "02",
        futanJuryo: "26C",
        kakuteiChakujun: "08",
        keibajoCode: "83",
        kohan3f: "000",
        kyori: "200",
        sohaTime: "3500",
        umaban: "02",
      }),
    ],
    runners: [],
    stats: stats({
      averageKohan3f: 360,
      fastestKohan3f: 340,
      medianKohan3f: 355,
    }),
  });
  if (drawn === null) {
    throw new Error("expected a Ban-ei scatter");
  }
  expect(drawn.xAxisTitle).toBe("斤量（右が重い）");
  expect(drawn.references.map((line) => line.kind)).toStrictEqual([
    "fastestRaceTime",
    "averageRaceTime",
    "medianRaceTime",
  ]);
  const weightWinner = drawn.points.find((point) => point.horseName === "1着馬");
  const weightLast = drawn.points.find((point) => point.horseName === "8着馬");
  if (weightWinner === undefined || weightLast === undefined) {
    throw new Error("expected Ban-ei finish points");
  }
  expect(weightLast.x > weightWinner.x).toBe(true);
  expect(weightWinner.radius === 10).toBe(true);
  expect(weightLast.radius === 5.6).toBe(true);
  expect(weightWinner.kohan3fTenths).toBe(null);
  expect(weightWinner.carriedWeightKg).toBe(610);
  expect(formatRaceTimeChartTooltip(weightWinner)).toStrictEqual([
    "1 1着馬",
    "2026-03-22",
    "帯広(ばんえい)",
    "過去騎手 騎手",
    "予定騎手 騎手",
    "距離 200m",
    "着順 1",
    "斤量 610kg",
    "レースタイム 3:18.8",
  ]);
});

it("connects multiple Ban-ei races of the same horse", () => {
  const drawn = buildDrawnBanEiAbilityChart({
    currentDistance: "200",
    keibajoCode: "83",
    results: [
      result({
        bamei: "連線馬",
        futanJuryo: "262",
        kaisaiTsukihi: "0322",
        kakuteiChakujun: "03",
        keibajoCode: "83",
        kohan3f: "000",
        kyori: "200",
        sohaTime: "3188",
      }),
      result({
        bamei: "連線馬",
        futanJuryo: "26C",
        kaisaiTsukihi: "0401",
        kakuteiChakujun: "01",
        keibajoCode: "83",
        kohan3f: "000",
        kyori: "200",
        sohaTime: "3300",
      }),
    ],
    runners: [],
    stats: null,
  });
  if (drawn === null) {
    throw new Error("expected Ban-ei horse links");
  }
  expect(drawn.horseLinks.length).toBe(1);
  expect(drawn.horseLinks[0]?.umaban).toBe("1");
  expect(drawn.horseLinks[0]?.stroke).toBe("#e6194b");
  expect(drawn.horseLinks[0]?.path.startsWith("M ")).toBe(true);
  const older = drawn.points.find((point) => point.sortKey === "202603228301");
  const newer = drawn.points.find((point) => point.sortKey === "202604018301");
  if (older === undefined || newer === undefined) {
    throw new Error("expected older and newer Ban-ei points");
  }
  expect(older.isLatest).toBe(false);
  expect(newer.isLatest).toBe(true);
});

it("drops a Ban-ei result without a finish rank", () => {
  const drawn = buildDrawnBanEiAbilityChart({
    currentDistance: "200",
    keibajoCode: "83",
    results: [
      result({
        kakuteiChakujun: "00",
        keibajoCode: "83",
        kohan3f: "000",
        kyori: "200",
        sohaTime: "3188",
      }),
    ],
    runners: [],
    stats: null,
  });
  expect(drawn).toBe(null);
});

it("drops a Ban-ei result without a carried weight", () => {
  const drawn = buildDrawnBanEiAbilityChart({
    currentDistance: "200",
    keibajoCode: "83",
    results: [
      result({
        futanJuryo: "FFF",
        keibajoCode: "83",
        kohan3f: "000",
        kyori: "200",
        sohaTime: "3188",
      }),
    ],
    runners: [],
    stats: null,
  });
  expect(drawn).toBe(null);
});

it("links a Ban-ei past weight to a heavier scheduled weight", () => {
  const drawn = buildDrawnBanEiAbilityChart({
    currentDistance: "200",
    keibajoCode: "83",
    results: [
      result({
        futanJuryo: "262",
        keibajoCode: "83",
        kohan3f: "000",
        kyori: "200",
        sohaTime: "3188",
      }),
    ],
    runners: [runner({ futanJuryo: "26C" })],
    stats: null,
  });
  if (drawn === null) {
    throw new Error("expected scheduled weight marks");
  }
  expect(drawn.scheduledGuides.length).toBe(1);
  expect(drawn.scheduledGuides[0]?.label).toBe("予定斤量 620kg");
  expect(drawn.scheduledMarks.length).toBe(1);
  expect(drawn.weightLinks.length).toBe(1);
  const past = drawn.points[0];
  const scheduled = drawn.scheduledMarks[0];
  const link = drawn.weightLinks[0];
  if (past === undefined || scheduled === undefined || link === undefined) {
    throw new Error("expected past and scheduled Ban-ei marks");
  }
  expect(scheduled.x > past.x).toBe(true);
  expect(link.stroke).toBe("#b45309");
  expect(link.x1).toBe(past.x);
  expect(link.x2).toBe(scheduled.x);
  expect(past.scheduledCarriedWeightKg).toBe(620);
  expect(past.carriedWeightDeltaKg).toBe(-10);
  expect(formatRaceTimeChartTooltip(past)).toStrictEqual([
    "1 テストホース",
    "2026-03-22",
    "帯広(ばんえい)",
    "過去騎手 騎手",
    "予定騎手 騎手",
    "距離 200m",
    "着順 1",
    "斤量 610kg",
    "予定斤量 620kg",
    "斤量差 -10kg",
    "レースタイム 3:18.8",
  ]);
});

it("omits a Ban-ei weight link when the scheduled load matches the past load", () => {
  const drawn = buildDrawnBanEiAbilityChart({
    currentDistance: "200",
    keibajoCode: "83",
    results: [
      result({
        futanJuryo: "262",
        keibajoCode: "83",
        kohan3f: "000",
        kyori: "200",
        sohaTime: "3188",
      }),
    ],
    runners: [runner({ futanJuryo: "262" })],
    stats: null,
  });
  if (drawn === null) {
    throw new Error("expected matching scheduled weight");
  }
  expect(drawn.scheduledMarks.length).toBe(0);
  expect(drawn.weightLinks.length).toBe(0);
  expect(drawn.scheduledGuides[0]?.label).toBe("予定斤量 610kg");
  expect(drawn.points[0]?.carriedWeightDeltaKg).toBe(0);
  const matched = drawn.points[0];
  if (matched === undefined) {
    throw new Error("expected a matched Ban-ei point");
  }
  expect(formatRaceTimeChartTooltip(matched)).toStrictEqual([
    "1 テストホース",
    "2026-03-22",
    "帯広(ばんえい)",
    "過去騎手 騎手",
    "予定騎手 騎手",
    "距離 200m",
    "着順 1",
    "斤量 610kg",
    "予定斤量 610kg",
    "斤量差 ±0kg",
    "レースタイム 3:18.8",
  ]);
});

it("does not draw one Ban-ei scheduled guide when horses have different loads", () => {
  const drawn = buildDrawnBanEiAbilityChart({
    currentDistance: "200",
    keibajoCode: "83",
    results: [
      result({
        futanJuryo: "262",
        keibajoCode: "83",
        kohan3f: "000",
        kyori: "200",
        sohaTime: "3188",
      }),
      result({
        currentUmaban: "02",
        futanJuryo: "262",
        keibajoCode: "83",
        kohan3f: "000",
        kyori: "200",
        sohaTime: "3300",
        umaban: "02",
      }),
    ],
    runners: [runner({ futanJuryo: "26C" }), runner({ futanJuryo: "276", umaban: "02" })],
    stats: null,
  });
  if (drawn === null) {
    throw new Error("expected mixed scheduled weights");
  }
  expect(drawn.scheduledGuides.length).toBe(0);
  expect(drawn.scheduledMarks.length).toBe(2);
  expect(drawn.weightLinks.length).toBe(2);
});

it("skips Ban-ei runners without a usable scheduled weight", () => {
  const drawn = buildDrawnBanEiAbilityChart({
    currentDistance: "200",
    keibajoCode: "83",
    results: [
      result({
        futanJuryo: "262",
        keibajoCode: "83",
        kohan3f: "000",
        kyori: "200",
        sohaTime: "3188",
      }),
    ],
    runners: [runner({ futanJuryo: "FFF" }), runner({ futanJuryo: "262", umaban: "00" })],
    stats: null,
  });
  if (drawn === null) {
    throw new Error("expected past-only Ban-ei marks");
  }
  expect(drawn.scheduledMarks.length).toBe(0);
  expect(drawn.weightLinks.length).toBe(0);
  expect(drawn.scheduledGuides.length).toBe(0);
  expect(drawn.points[0]?.scheduledCarriedWeightKg).toBe(null);
});
