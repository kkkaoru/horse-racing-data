// This file runs with bun.

import { expect, it } from "vitest";

import type { Training } from "./race-types";
import {
  buildDrawnTrainingChart,
  buildDrawnTrainingTrendChart,
  collectTrainingScatterRows,
  DEFAULT_SHOW_ALL_TRAINING_WORKOUTS,
  DEFAULT_SHOW_TRAINING_CHART,
  deriveTrainingSplitSeconds,
  formatTrainingChartTooltip,
  formatTrainingPaceVsEven,
  formatTrainingPlaceSummary,
  parseTrainingSeconds,
  TRAINING_CHART_EVEN_PACE_LABEL,
  TRAINING_CHART_EVEN_PACE_RATIO,
  TRAINING_CHART_NOTE,
  TRAINING_CHART_PLOT_BOTTOM,
  TRAINING_CHART_PLOT_LEFT,
  TRAINING_CHART_PLOT_RIGHT,
  TRAINING_CHART_PLOT_TOP,
  TRAINING_CHART_TOOLTIP_OFFSET,
  TRAINING_CHART_VIEW_HEIGHT,
  TRAINING_CHART_VIEW_WIDTH,
  TRAINING_CHART_X_AXIS_TITLE,
  TRAINING_CHART_Y_AXIS_TITLE,
  TRAINING_FURLONG_COLUMNS,
  TRAINING_TREND_CHART_NOTE,
  TRAINING_TREND_LINE_STROKE,
  TRAINING_TREND_X_AXIS_TITLE,
  TRAINING_TREND_Y_AXIS_TITLE,
  trainingChartFrameOrigin,
  trainingChartTooltipPosition,
  trainingGradeStroke,
} from "./training-charts";

const training = (overrides: Partial<Training>): Training => ({
  babamawari: "1",
  bamei: "テストホース",
  chokyoJikoku: "0600",
  chokyoNengappi: "20260516",
  course: "1",
  currentJockeyName: "騎手",
  lapTime10f: null,
  lapTime1f: "120",
  lapTime2f: null,
  lapTime3f: null,
  lapTime4f: null,
  lapTime5f: null,
  lapTime6f: null,
  lapTime7f: null,
  lapTime8f: null,
  lapTime9f: null,
  premiumEvaluationGrade: null,
  premiumEvaluationText: null,
  timeGokei10f: null,
  timeGokei2f: "240",
  timeGokei3f: "360",
  timeGokei4f: "480",
  timeGokei5f: null,
  timeGokei6f: null,
  timeGokei7f: null,
  timeGokei8f: null,
  timeGokei9f: null,
  tracenKubun: "1",
  trainerName: "調教師",
  trainingRiderName: "騎乗者",
  trainingType: "坂路",
  umaban: "01",
  ...overrides,
});

it("defaults the training view to the chart", () => {
  expect(DEFAULT_SHOW_TRAINING_CHART).toBe(true);
  expect(DEFAULT_SHOW_ALL_TRAINING_WORKOUTS).toBe(false);
  expect(TRAINING_CHART_NOTE).toBe(
    "点線の十字は均等ペースです。上ほど最終1Fが自分の均等より速く、右ほど最終2Fが自分の均等より速い。2Fがない調教は3Fで見ます。コースが違う時計は直接比べません。点の色は調教記号、数字は馬番。コースはホバーで確認。",
  );
  expect(TRAINING_TREND_CHART_NOTE).toBe(
    "馬ごとの行は最終1Fの時系列です。右が新しい調教。各行の上へ向かうほど、その調教の均等ペースより速い。線は位置、丸の色は調教記号。ホバーで時計と最終2F。",
  );
});

it("parses training times in tenths into seconds", () => {
  expect(parseTrainingSeconds("120")).toBe(12);
  expect(parseTrainingSeconds("498")).toBe(49.8);
});

it("treats missing training times as null", () => {
  expect(parseTrainingSeconds(null)).toBe(null);
  expect(parseTrainingSeconds("")).toBe(null);
  expect(parseTrainingSeconds("000")).toBe(null);
  expect(parseTrainingSeconds("999")).toBe(null);
});

it("derives a positive remaining-distance interval split", () => {
  expect(deriveTrainingSplitSeconds(36, 24)).toBe(12);
  expect(deriveTrainingSplitSeconds(24, 12)).toBe(12);
});

it("returns null for missing or non-positive interval splits", () => {
  expect(deriveTrainingSplitSeconds(null, 12)).toBe(null);
  expect(deriveTrainingSplitSeconds(24, null)).toBe(null);
  expect(deriveTrainingSplitSeconds(12, 12)).toBe(null);
  expect(deriveTrainingSplitSeconds(11, 12)).toBe(null);
});

it("joins place, type, and course for a training summary", () => {
  expect(formatTrainingPlaceSummary(training({}))).toBe("栗東 / 坂路 / Aコース / 外");
});

it("returns a dash when a training has no place details", () => {
  expect(
    formatTrainingPlaceSummary(
      training({
        babamawari: "",
        course: "",
        tracenKubun: "",
        trainingType: "-",
      }),
    ),
  ).toBe("-");
});

it("maps evaluation grades to stroke colors", () => {
  expect(trainingGradeStroke("◎")).toBe("#b45309");
  expect(trainingGradeStroke("ss")).toBe("#b45309");
  expect(trainingGradeStroke("S")).toBe("#b45309");
  expect(trainingGradeStroke("1")).toBe("#b45309");
  expect(trainingGradeStroke("○")).toBe("#166534");
  expect(trainingGradeStroke("◯")).toBe("#166534");
  expect(trainingGradeStroke("A")).toBe("#166534");
  expect(trainingGradeStroke("2")).toBe("#166534");
  expect(trainingGradeStroke("▲")).toBe("#c2410c");
  expect(trainingGradeStroke("B")).toBe("#c2410c");
  expect(trainingGradeStroke("3")).toBe("#c2410c");
  expect(trainingGradeStroke("△")).toBe("#355f9f");
  expect(trainingGradeStroke("C")).toBe("#355f9f");
  expect(trainingGradeStroke("4")).toBe("#355f9f");
  expect(trainingGradeStroke("")).toBe("#64748b");
  expect(trainingGradeStroke("Z")).toBe("#64748b");
});

it("returns null when no 1F versus 4F/3F points can be plotted", () => {
  expect(buildDrawnTrainingChart({ trainings: [] })).toBe(null);
  expect(
    buildDrawnTrainingChart({
      trainings: [
        training({
          lapTime1f: "000",
          timeGokei2f: null,
          timeGokei3f: null,
          timeGokei4f: null,
        }),
      ],
    }),
  ).toBe(null);
});

it("skips a horse that has no 1F even when 4F exists", () => {
  expect(
    collectTrainingScatterRows([
      training({
        lapTime1f: "000",
        timeGokei4f: "480",
      }),
    ]),
  ).toStrictEqual([]);
});

it("skips a horse that has 1F but neither 4F nor 3F", () => {
  expect(
    collectTrainingScatterRows([
      training({
        lapTime1f: "120",
        timeGokei3f: null,
        timeGokei4f: "000",
      }),
    ]),
  ).toStrictEqual([]);
});

it("uses 4F even pace when 4F is present", () => {
  const rows = collectTrainingScatterRows([
    training({
      lapTime1f: "120",
      timeGokei2f: "240",
      timeGokei3f: "360",
      timeGokei4f: "480",
    }),
  ]);
  expect(rows.length).toBe(1);
  expect(rows[0]?.evenPaceFurlongs).toBe(4);
  expect(rows[0]?.evenPace1FSeconds).toBe(12);
  expect(rows[0]?.kireruRatio).toBe(1);
  expect(rows[0]?.sustainRatio).toBe(1);
  expect(rows[0]?.oneFSeconds).toBe(12);
  expect(rows[0]?.split32).toBe(12);
  expect(rows[0]?.split21).toBe(12);
});

it("falls back to 3F even pace when 4F is missing", () => {
  const rows = collectTrainingScatterRows([
    training({
      lapTime1f: "108",
      timeGokei2f: "216",
      timeGokei3f: "360",
      timeGokei4f: "000",
    }),
  ]);
  expect(rows.length).toBe(1);
  expect(rows[0]?.evenPaceFurlongs).toBe(3);
  expect(rows[0]?.evenPace1FSeconds).toBe(12);
  expect(rows[0]?.kireruRatio).toBe(0.9);
  expect(rows[0]?.sustainRatio).toBe(0.9);
  expect(rows[0]?.fourFSeconds).toBe(null);
  expect(rows[0]?.threeFSeconds).toBe(36);
});

it("computes 切れ味 as last 1F over even-pace 1F", () => {
  const rows = collectTrainingScatterRows([
    training({
      lapTime1f: "108",
      timeGokei2f: "240",
      timeGokei4f: "480",
    }),
  ]);
  expect(rows[0]?.kireruRatio).toBe(0.9);
  expect(rows[0]?.sustainRatio).toBe(1);
});

it("computes 持続 from last 2F over even-pace 2F", () => {
  const rows = collectTrainingScatterRows([
    training({
      lapTime1f: "120",
      timeGokei2f: "216",
      timeGokei4f: "480",
    }),
  ]);
  expect(rows[0]?.kireruRatio).toBe(1);
  expect(rows[0]?.sustainRatio).toBe(0.9);
});

it("computes 持続 from last 3F when 2F is missing", () => {
  const rows = collectTrainingScatterRows([
    training({
      lapTime1f: "120",
      timeGokei2f: null,
      timeGokei3f: "270",
      timeGokei4f: "480",
    }),
  ]);
  expect(rows[0]?.sustainRatio).toBe(0.75);
  expect(rows[0]?.kireruRatio).toBe(1);
  expect(rows[0]?.twoFSeconds).toBe(null);
});

it("uses even-pace total as 持続 when 2F and 3F are missing", () => {
  const rows = collectTrainingScatterRows([
    training({
      lapTime1f: "108",
      timeGokei2f: null,
      timeGokei3f: null,
      timeGokei4f: "480",
    }),
  ]);
  expect(rows[0]?.evenPaceFurlongs).toBe(4);
  expect(rows[0]?.kireruRatio).toBe(0.9);
  expect(rows[0]?.sustainRatio).toBe(1);
});

it("keeps every valid workout for the same horse in recency order", () => {
  const rows = collectTrainingScatterRows([
    training({
      bamei: "同じ馬",
      chokyoJikoku: "0600",
      chokyoNengappi: "20260516",
      lapTime1f: "120",
      timeGokei4f: "480",
      umaban: "01",
    }),
    training({
      bamei: "同じ馬",
      chokyoJikoku: "0500",
      chokyoNengappi: "20260510",
      lapTime1f: "130",
      timeGokei4f: "500",
      umaban: "01",
    }),
  ]);
  expect(rows.length).toBe(2);
  expect(rows[0]?.oneFSeconds).toBe(13);
  expect(rows[0]?.recencyKey).toBe("202605100500");
  expect(rows[1]?.oneFSeconds).toBe(12);
  expect(rows[1]?.recencyKey).toBe("202605160600");
});

it("keeps both 坂路 and ウッド workouts for the same horse", () => {
  const rows = collectTrainingScatterRows([
    training({
      bamei: "同じ馬",
      chokyoNengappi: "20260510",
      trainingType: "坂路",
      umaban: "01",
    }),
    training({
      bamei: "同じ馬",
      chokyoNengappi: "20260516",
      course: "1",
      trainingType: "ウッド",
      umaban: "01",
    }),
  ]);
  expect(rows.length).toBe(2);
  expect(rows[0]?.courseFacet).toBe("坂路");
  expect(rows[1]?.courseFacet).toBe("ウッド");
});

it("keeps only the newest workout on the comparison scatter", () => {
  const drawn = buildDrawnTrainingChart({
    trainings: [
      training({
        chokyoNengappi: "20260510",
        lapTime1f: "130",
        timeGokei4f: "500",
        umaban: "01",
      }),
      training({
        chokyoNengappi: "20260516",
        lapTime1f: "120",
        timeGokei4f: "480",
        umaban: "01",
      }),
    ],
  });
  expect(drawn === null).toBe(false);
  if (drawn === null) {
    throw new Error("expected a training chart");
  }
  expect(drawn.points.length).toBe(1);
  expect(drawn.points[0]?.isLatest).toBe(true);
  expect(drawn.points[0]?.recencyKey).toBe("202605160600");
  expect(drawn.series.length).toBe(0);
});

it("plots a faster last 1F higher and a faster late section to the right", () => {
  const drawn = buildDrawnTrainingChart({
    trainings: [
      training({
        bamei: "鈍い馬",
        lapTime1f: "150",
        timeGokei2f: "300",
        timeGokei4f: "480",
        umaban: "01",
      }),
      training({
        bamei: "鋭い馬",
        lapTime1f: "90",
        timeGokei2f: "180",
        timeGokei4f: "480",
        umaban: "02",
      }),
    ],
  });
  expect(drawn === null).toBe(false);
  if (drawn === null) {
    throw new Error("expected a training scatter");
  }
  expect(drawn.points.length).toBe(2);
  expect(drawn.xAxisTitle).toBe("最終2F（右が均等より速い）");
  expect(drawn.yAxisTitle).toBe("最終1F（上が均等より速い）");
  expect(drawn.evenLabel).toBe("均等ペース");
  expect(drawn.width).toBe(720);
  expect(drawn.height).toBe(380);
  const dull = drawn.points.find((point) => point.horseName === "鈍い馬");
  const sharp = drawn.points.find((point) => point.horseName === "鋭い馬");
  if (dull === undefined || sharp === undefined) {
    throw new Error("expected dull and sharp points");
  }
  expect(sharp.kireruRatio).toBe(0.75);
  expect(dull.kireruRatio).toBe(1.25);
  expect(sharp.sustainRatio).toBe(0.75);
  expect(dull.sustainRatio).toBe(1.25);
  expect(sharp.x > dull.x).toBe(true);
  expect(sharp.y < dull.y).toBe(true);
  expect(sharp.x > 72).toBe(true);
  expect(sharp.x < 704).toBe(true);
  expect(sharp.y > 20).toBe(true);
  expect(sharp.y < 332).toBe(true);
});

it("plots 坂路 and ウッド horses on one scatter using ratios not raw clocks", () => {
  const drawn = buildDrawnTrainingChart({
    trainings: [
      training({
        bamei: "坂路馬",
        lapTime1f: "130",
        timeGokei2f: "260",
        timeGokei4f: "520",
        trainingType: "坂路",
        umaban: "01",
      }),
      training({
        bamei: "ウッド馬",
        lapTime1f: "120",
        timeGokei2f: "240",
        timeGokei4f: "520",
        trainingType: "ウッド",
        umaban: "02",
      }),
    ],
  });
  if (drawn === null) {
    throw new Error("expected one scatter");
  }
  expect(drawn.points.length).toBe(2);
  const sakaji = drawn.points.find((point) => point.horseName === "坂路馬");
  const wood = drawn.points.find((point) => point.horseName === "ウッド馬");
  if (sakaji === undefined || wood === undefined) {
    throw new Error("expected both course horses");
  }
  expect(sakaji.fourFSeconds).toBe(52);
  expect(wood.fourFSeconds).toBe(52);
  expect(sakaji.kireruRatio).toBe(1);
  expect(wood.kireruRatio).toBe(0.9230769230769231);
  expect(wood.y < sakaji.y).toBe(true);
  expect(sakaji.courseFacet).toBe("坂路");
  expect(wood.courseFacet).toBe("ウッド");
});

it("keeps 3F-only and 4F horses on the same chart", () => {
  const drawn = buildDrawnTrainingChart({
    trainings: [
      training({
        bamei: "4F馬",
        timeGokei4f: "480",
        umaban: "01",
      }),
      training({
        bamei: "3F馬",
        timeGokei4f: null,
        umaban: "02",
      }),
    ],
  });
  if (drawn === null) {
    throw new Error("expected mixed even-pace horses");
  }
  expect(drawn.points.length).toBe(2);
  expect(drawn.points.find((point) => point.horseName === "4F馬")?.evenPaceFurlongs).toBe(4);
  expect(drawn.points.find((point) => point.horseName === "3F馬")?.evenPaceFurlongs).toBe(3);
});

it("pads the ratio domain when every split is even", () => {
  const drawn = buildDrawnTrainingChart({ trainings: [training({})] });
  if (drawn === null) {
    throw new Error("expected a training scatter");
  }
  expect(drawn.xTicks[0]?.label).toBe("1.05");
  expect(drawn.xTicks[4]?.label).toBe("0.95");
  expect(drawn.yTicks[0]?.label).toBe("1.05");
  expect(drawn.yTicks[4]?.label).toBe("0.95");
});

it("pads a non-zero ratio span and still includes even pace", () => {
  const drawn = buildDrawnTrainingChart({
    trainings: [
      training({
        bamei: "均等馬",
        lapTime1f: "120",
        timeGokei2f: "240",
        umaban: "01",
      }),
      training({
        bamei: "切れ馬",
        lapTime1f: "108",
        timeGokei2f: "216",
        umaban: "02",
      }),
    ],
  });
  if (drawn === null) {
    throw new Error("expected a padded scatter");
  }
  expect(drawn.xTicks[0]?.label).toBe("1.01");
  expect(drawn.yTicks[0]?.label).toBe("1.01");
  expect(drawn.evenX > 72).toBe(true);
  expect(drawn.evenX < 704).toBe(true);
  expect(drawn.evenY > 20).toBe(true);
  expect(drawn.evenY < 332).toBe(true);
});

it("colors a point by its evaluation grade", () => {
  const drawn = buildDrawnTrainingChart({
    trainings: [training({ premiumEvaluationGrade: "A" })],
  });
  if (drawn === null) {
    throw new Error("expected a training scatter");
  }
  expect(drawn.points[0]?.stroke).toBe("#166534");
});

it("draws weaker grades behind stronger grades", () => {
  const drawn = buildDrawnTrainingChart({
    trainings: [
      training({
        bamei: "上位",
        premiumEvaluationGrade: "S",
        umaban: "02",
      }),
      training({
        bamei: "下位",
        premiumEvaluationGrade: "C",
        umaban: "01",
      }),
    ],
  });
  if (drawn === null) {
    throw new Error("expected a training scatter");
  }
  expect(drawn.points[0]?.horseName).toBe("下位");
  expect(drawn.points[1]?.horseName).toBe("上位");
});

it("draws missing grades behind unknown grades", () => {
  const drawn = buildDrawnTrainingChart({
    trainings: [
      training({
        bamei: "未知記号",
        premiumEvaluationGrade: "Z",
        umaban: "02",
      }),
      training({
        bamei: "記号なし馬",
        premiumEvaluationGrade: "",
        umaban: "01",
      }),
    ],
  });
  if (drawn === null) {
    throw new Error("expected a training scatter");
  }
  expect(drawn.points[0]?.horseName).toBe("記号なし馬");
  expect(drawn.points[1]?.horseName).toBe("未知記号");
});

it("uses horse id as a tie-breaker when grades match", () => {
  const drawn = buildDrawnTrainingChart({
    trainings: [
      training({
        bamei: "後の馬",
        umaban: "02",
      }),
      training({
        bamei: "先の馬",
        umaban: "01",
      }),
    ],
  });
  if (drawn === null) {
    throw new Error("expected a training scatter");
  }
  expect(drawn.points[0]?.horseName).toBe("先の馬");
  expect(drawn.points[1]?.horseName).toBe("後の馬");
});

it("formats a tooltip with horse, date, course, grade, 6F to 1F, splits, and ratios", () => {
  const rows = collectTrainingScatterRows([
    training({
      bamei: "ツールホース",
      premiumEvaluationGrade: "A",
      premiumEvaluationText: "動き良い",
      timeGokei5f: "600",
      timeGokei6f: "720",
    }),
  ]);
  if (rows[0] === undefined) {
    throw new Error("expected a scatter row");
  }
  expect(formatTrainingChartTooltip(rows[0])).toStrictEqual([
    "1 ツールホース",
    "2026-05-16",
    "栗東 / 坂路 / Aコース / 外",
    "評価 A 動き良い",
    "6F 72.0",
    "5F 60.0",
    "4F 48.0",
    "3F 36.0",
    "2F 24.0",
    "1F 12.0",
    "3-2 12.0",
    "2-1 12.0",
    "最終1F 1.00（均等）",
    "最終2F 1.00（均等）",
  ]);
});

it("omits missing interval splits and marks missing clocks in the tooltip", () => {
  const rows = collectTrainingScatterRows([
    training({
      bamei: "3Fのみ",
      premiumEvaluationGrade: "B",
      timeGokei2f: null,
      timeGokei4f: null,
    }),
  ]);
  if (rows[0] === undefined) {
    throw new Error("expected a 3F fallback row");
  }
  expect(formatTrainingChartTooltip(rows[0])).toStrictEqual([
    "1 3Fのみ",
    "2026-05-16",
    "栗東 / 坂路 / Aコース / 外",
    "評価 B -",
    "6F -",
    "5F -",
    "4F -",
    "3F 36.0",
    "2F -",
    "1F 12.0",
    "最終1F 1.00（均等）",
    "最終3F 1.00（均等）",
  ]);
});

it("uses a dash for an undated workout in the tooltip", () => {
  const rows = collectTrainingScatterRows([
    training({
      chokyoNengappi: "",
      premiumEvaluationGrade: "B",
    }),
  ]);
  if (rows[0] === undefined) {
    throw new Error("expected a scatter row");
  }
  expect(rows[0].dateLabel).toBe("-");
  expect(formatTrainingChartTooltip(rows[0])[1]).toBe("-");
});

it("keeps a netkeiba workout index in the point id", () => {
  const rows = collectTrainingScatterRows([training({ premiumWorkoutIndex: 0 })]);
  if (rows[0] === undefined) {
    throw new Error("expected a scatter row");
  }
  expect(rows[0].id).toBe("01-坂路-20260516-0600-0");
});

it("still plots a blank training type without splitting the chart", () => {
  const drawn = buildDrawnTrainingChart({
    trainings: [
      training({
        trainingType: "-",
      }),
    ],
  });
  if (drawn === null) {
    throw new Error("expected an unknown-course point");
  }
  expect(drawn.points.length).toBe(1);
  expect(drawn.points[0]?.courseFacet).toBe("コース不明");
});

it("still plots an empty training type without splitting the chart", () => {
  const drawn = buildDrawnTrainingChart({
    trainings: [
      training({
        trainingType: "",
      }),
    ],
  });
  if (drawn === null) {
    throw new Error("expected an empty-type point");
  }
  expect(drawn.points.length).toBe(1);
  expect(drawn.points[0]?.courseFacet).toBe("コース不明");
});

it("places the HTML tooltip beside the pointer", () => {
  expect(
    trainingChartTooltipPosition({
      clientX: 40,
      clientY: 48,
      frameLeft: 20,
      frameTop: 10,
    }),
  ).toStrictEqual({
    x: 32,
    y: 50,
  });
  expect(TRAINING_CHART_TOOLTIP_OFFSET).toBe(12);
});

it("reads the chart frame origin or falls back to the top-left", () => {
  expect(trainingChartFrameOrigin(null)).toStrictEqual({ left: 0, top: 0 });
  expect(
    trainingChartFrameOrigin({
      getBoundingClientRect: () => ({ left: 20, top: 10 }),
    }),
  ).toStrictEqual({ left: 20, top: 10 });
});

it("keeps the first workout when two records share the same recency", () => {
  const rows = collectTrainingScatterRows([
    training({
      bamei: "先に来た調教",
      chokyoJikoku: "0600",
      chokyoNengappi: "20260516",
      umaban: "01",
    }),
    training({
      bamei: "同じ時刻の調教",
      chokyoJikoku: "0600",
      chokyoNengappi: "20260516",
      umaban: "01",
    }),
  ]);
  expect(rows.length).toBe(1);
  expect(rows[0]?.horseName).toBe("先に来た調教");
});

it("uses a jra suffix when a workout has no premium index", () => {
  const rows = collectTrainingScatterRows([training({})]);
  expect(rows[0]?.id).toBe("01-坂路-20260516-0600-jra");
});

it("exports the default furlong columns from 6F to 1F", () => {
  expect(TRAINING_FURLONG_COLUMNS).toStrictEqual([
    { key: "timeGokei6f", label: "6F" },
    { key: "timeGokei5f", label: "5F" },
    { key: "timeGokei4f", label: "4F" },
    { key: "timeGokei3f", label: "3F" },
    { key: "timeGokei2f", label: "2F" },
    { key: "lapTime1f", label: "1F" },
  ]);
  expect(TRAINING_CHART_VIEW_WIDTH).toBe(720);
  expect(TRAINING_CHART_VIEW_HEIGHT).toBe(380);
  expect(TRAINING_CHART_PLOT_LEFT).toBe(72);
  expect(TRAINING_CHART_PLOT_RIGHT).toBe(704);
  expect(TRAINING_CHART_PLOT_TOP).toBe(20);
  expect(TRAINING_CHART_PLOT_BOTTOM).toBe(332);
  expect(TRAINING_CHART_X_AXIS_TITLE).toBe("最終2F（右が均等より速い）");
  expect(TRAINING_CHART_Y_AXIS_TITLE).toBe("最終1F（上が均等より速い）");
  expect(TRAINING_CHART_EVEN_PACE_LABEL).toBe("均等ペース");
  expect(TRAINING_CHART_EVEN_PACE_RATIO).toBe(1);
});

it("draws one trend lane per horse with every workout", () => {
  const drawn = buildDrawnTrainingTrendChart([
    training({
      chokyoNengappi: "20260504",
      lapTime1f: "140",
      timeGokei4f: "520",
      umaban: "01",
    }),
    training({
      chokyoNengappi: "20260510",
      lapTime1f: "130",
      timeGokei4f: "500",
      umaban: "01",
    }),
    training({
      chokyoNengappi: "20260516",
      lapTime1f: "120",
      timeGokei4f: "480",
      umaban: "01",
    }),
    training({
      bamei: "単調教馬",
      umaban: "02",
    }),
  ]);
  if (drawn === null) {
    throw new Error("expected a trend chart");
  }
  expect(drawn.lanes.length).toBe(2);
  expect(drawn.lanes[0]?.umaban).toBe("1");
  expect(drawn.lanes[0]?.points.length).toBe(3);
  expect(drawn.lanes[0]?.path.slice(0, 1)).toBe("M");
  expect(drawn.lanes[0]?.points[2]?.isLatest).toBe(true);
  expect(drawn.lanes[0]?.points[0]?.isLatest).toBe(false);
  expect(drawn.lanes[1]?.points.length).toBe(1);
  expect(drawn.lanes[1]?.path).toBe("");
  expect(drawn.xAxisTitle).toBe("調教日（右が新しい）");
  expect(drawn.yAxisTitle).toBe("最終1F（各行の上が速い）");
  expect(TRAINING_TREND_LINE_STROKE).toBe("#d5ddd8");
  expect(TRAINING_TREND_X_AXIS_TITLE).toBe("調教日（右が新しい）");
  expect(TRAINING_TREND_Y_AXIS_TITLE).toBe("最終1F（各行の上が速い）");
});

it("returns no trend chart when no 1F versus 4F/3F points exist", () => {
  expect(buildDrawnTrainingTrendChart([])).toBe(null);
  expect(
    buildDrawnTrainingTrendChart([
      training({
        lapTime1f: "000",
        timeGokei2f: null,
        timeGokei3f: null,
        timeGokei4f: null,
      }),
    ]),
  ).toBe(null);
});

it("pads a single-day trend domain and places a later workout to the right", () => {
  const drawn = buildDrawnTrainingTrendChart([
    training({
      chokyoNengappi: "20260516",
      chokyoJikoku: "0500",
      lapTime1f: "130",
      timeGokei4f: "500",
    }),
    training({
      chokyoNengappi: "20260516",
      chokyoJikoku: "0600",
      lapTime1f: "120",
      timeGokei4f: "480",
    }),
  ]);
  if (drawn === null) {
    throw new Error("expected a same-day trend");
  }
  expect(drawn.lanes[0]?.points.length).toBe(2);
  expect(drawn.lanes[0]?.points[1]?.x ?? 0).toBeGreaterThan(drawn.lanes[0]?.points[0]?.x ?? 0);
});

it("places an undated workout at the left of the trend", () => {
  const drawn = buildDrawnTrainingTrendChart([
    training({
      chokyoNengappi: "",
      chokyoJikoku: "",
    }),
    training({}),
  ]);
  if (drawn === null) {
    throw new Error("expected an undated trend point");
  }
  expect(drawn.lanes[0]?.points.length).toBe(2);
  expect(drawn.lanes[0]?.points[0]?.x ?? 1).toBeLessThan(drawn.lanes[0]?.points[1]?.x ?? 0);
});

it("treats a non-numeric training date as the left edge", () => {
  const drawn = buildDrawnTrainingTrendChart([
    training({
      chokyoNengappi: "abcdefgh",
    }),
    training({}),
  ]);
  if (drawn === null) {
    throw new Error("expected a non-numeric date trend");
  }
  expect(drawn.lanes[0]?.points[1]?.recencyKey).toBe("abcdefgh0600");
  expect((drawn.lanes[0]?.points[1]?.x ?? 1) < (drawn.lanes[0]?.points[0]?.x ?? 0)).toBe(true);
});

it("pads the trend x domain when every workout shares a timestamp", () => {
  const drawn = buildDrawnTrainingTrendChart([
    training({ trainingType: "坂路" }),
    training({ course: "1", trainingType: "ウッド" }),
  ]);
  if (drawn === null) {
    throw new Error("expected a shared-timestamp trend");
  }
  expect(drawn.lanes[0]?.points.length).toBe(2);
  expect(drawn.xTicks.length).toBe(5);
});

it("names even, faster, and slower ratios against even pace", () => {
  expect(formatTrainingPaceVsEven(1)).toBe("均等");
  expect(formatTrainingPaceVsEven(0.75)).toBe("均等より速い");
  expect(formatTrainingPaceVsEven(1.25)).toBe("均等より遅い");
});

it("says the tooltip last 1F is faster than even pace", () => {
  const rows = collectTrainingScatterRows([
    training({
      lapTime1f: "90",
      timeGokei2f: "180",
      timeGokei4f: "480",
    }),
  ]);
  if (rows[0] === undefined) {
    throw new Error("expected a faster last 1F row");
  }
  expect(formatTrainingChartTooltip(rows[0])[12]).toBe("最終1F 0.75（均等より速い）");
  expect(formatTrainingChartTooltip(rows[0])[13]).toBe("最終2F 0.75（均等より速い）");
});
