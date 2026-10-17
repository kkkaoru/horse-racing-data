// This file runs with bun.

import { scaleLinear } from "d3";

import { cleanText, formatDate } from "./format";
import {
  horseRaceChartPaintForWakuban,
  type HorseRaceWakubanPaint,
} from "./horse-race-results-chart-data";
import {
  formatKohan3fTenthsLabel,
  formatRaceTimeTenthsLabel,
  parseKohan3fTenths,
  parseRaceChartDateValue,
  parseRaceDistanceMeters,
  parseRaceFinishRank,
  parseSohaTimeTenths,
  scaleSohaTimeToDistance,
  summariseNumbers,
} from "./horse-race-time-charts";
import type { HorseRaceResult, RaceTimeStats, RaceTimeTargetRace } from "./race-types";
import { formatRunnerNumber } from "./runner-format";

// Whether a past time was scaled from a shorter or longer race than today's.
export type ClockDistanceConversion = "longer" | "same" | "shorter";

export interface ClockAnalysisRow {
  dateLabel: string;
  dateValue: number;
  distanceConversion: ClockDistanceConversion;
  distanceMeters: number | null;
  fill: string;
  finishRank: number | null;
  horseName: string;
  kohan3fTenths: number | null;
  scaledTimeTenths: number;
  stroke: string;
  umaban: string;
}

export interface ClockPar {
  averageKohan3f: number | null;
  averageRaceTime: number | null;
  fastestKohan3f: number | null;
  fastestRaceTime: number | null;
  medianKohan3f: number | null;
  medianRaceTime: number | null;
  sampleCount: number;
}

export interface ClockAxisTick {
  label: string;
  x: number;
  y: number;
}

export interface ClockScatterPoint {
  dateLabel: string;
  distanceConversion: ClockDistanceConversion;
  distanceLabel: string;
  fill: string;
  horseName: string;
  id: string;
  kohanLabel: string;
  r: number;
  stroke: string;
  timeLabel: string;
  umaban: string;
  x: number;
  y: number;
}

export type ClockReferenceKind =
  | "averageKohan"
  | "averageTime"
  | "fastestKohan"
  | "fastestTime"
  | "medianKohan"
  | "medianTime";

export interface ClockReferenceLine {
  kind: ClockReferenceKind;
  label: string;
  stroke: string;
  strokeDasharray: string;
  x1: number;
  x2: number;
  y1: number;
  y2: number;
}

interface ClockReferenceStyle {
  stroke: string;
  strokeDasharray: string;
}

interface ClockPlacedReferenceParams {
  kind: ClockReferenceKind;
  label: string;
  orientation: "horizontal" | "vertical";
  value: number | null;
}

export interface ClockScatterView {
  height: number;
  points: ClockScatterPoint[];
  plotBottom: number;
  plotLeft: number;
  plotRight: number;
  plotTop: number;
  references: ClockReferenceLine[];
  title: string;
  width: number;
  xAxisTitle: string;
  xTicks: ClockAxisTick[];
  yAxisTitle: string;
  yTicks: ClockAxisTick[];
}

interface ClockFrameRunner {
  sourceHorseId?: string | null;
  bamei?: string | null;
  kettoTorokuBango: string | null;
  wakuban: string | null;
}

interface CollectAnalysisRowsParams {
  includeMissingKohan?: boolean;
  currentDistance: string | null | undefined;
  results: HorseRaceResult[];
  runners: ClockFrameRunner[];
}

interface BuildScaledWinnerParParams {
  currentDistance: string | null | undefined;
  races: RaceTimeTargetRace[];
}

interface ClockMarkerShapeParams {
  conversion: ClockDistanceConversion;
  r: number;
  x: number;
  y: number;
}

interface PlotBox {
  bottom: number;
  left: number;
  right: number;
  top: number;
}

const VIEW_WIDTH: number = 720;
const VIEW_HEIGHT: number = 360;
const PLOT: PlotBox = { bottom: 300, left: 72, right: 700, top: 24 };
const SCATTER_TITLE: string = "換算タイム×上がり3F";
const TICK_FRACTIONS: number[] = [0, 0.25, 0.5, 0.75, 1];
const POINT_RADIUS: number = 5;
const FASTEST_STROKE: string = "#be123c";
const AVERAGE_STROKE: string = "#166534";
const MEDIAN_STROKE: string = "#4338ca";
const RACE_TIME_DASH: string = "6 4";
const KOHAN_DASH: string = "2 3";
const CLOCK_REFERENCE_STYLE_BY_KIND: Record<ClockReferenceKind, ClockReferenceStyle> = {
  averageKohan: { stroke: AVERAGE_STROKE, strokeDasharray: KOHAN_DASH },
  averageTime: { stroke: AVERAGE_STROKE, strokeDasharray: RACE_TIME_DASH },
  fastestKohan: { stroke: FASTEST_STROKE, strokeDasharray: KOHAN_DASH },
  fastestTime: { stroke: FASTEST_STROKE, strokeDasharray: RACE_TIME_DASH },
  medianKohan: { stroke: MEDIAN_STROKE, strokeDasharray: KOHAN_DASH },
  medianTime: { stroke: MEDIAN_STROKE, strokeDasharray: RACE_TIME_DASH },
};
// A triangle's circumradius is enlarged so its area reads like the circle's.
const TRIANGLE_RADIUS_SCALE: number = 1.4;
const TRIANGLE_HALF_WIDTH_RATIO: number = Math.sqrt(3) / 2;
const TRIANGLE_BASE_OFFSET_RATIO: number = 0.5;
const MARKER_COORDINATE_DIGITS: number = 2;
export const CLOCK_DISTANCE_CONVERSION_LABELS: Record<ClockDistanceConversion, string> = {
  longer: "長い距離から換算",
  same: "同距離",
  shorter: "短い距離から換算",
};
export const CLOCK_DISTANCE_CONVERSIONS: readonly ClockDistanceConversion[] = [
  "same",
  "shorter",
  "longer",
];
const EMPTY_PAR: ClockPar = {
  averageKohan3f: null,
  averageRaceTime: null,
  fastestKohan3f: null,
  fastestRaceTime: null,
  medianKohan3f: null,
  medianRaceTime: null,
  sampleCount: 0,
};

const axisTicks = (
  scale: (value: number) => number,
  domain: [number, number],
  format: (value: number) => string,
  vertical: boolean,
): ClockAxisTick[] =>
  TICK_FRACTIONS.map((fraction) => {
    const value = domain[0] + (domain[1] - domain[0]) * fraction;
    return vertical
      ? { label: format(value), x: PLOT.left, y: scale(value) }
      : { label: format(value), x: scale(value), y: PLOT.bottom };
  });

const paddedDomain = (values: number[]): [number, number] => {
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const pad = minValue === maxValue ? 8 : (maxValue - minValue) * 0.08;
  return [minValue - pad, maxValue + pad];
};

const compareUmaban = (left: string, right: string): number => left.localeCompare(right, "ja");

const compareClockRows = (left: ClockAnalysisRow, right: ClockAnalysisRow): number => {
  const umaban = compareUmaban(left.umaban, right.umaban);
  return umaban === 0 ? left.dateValue - right.dateValue : umaban;
};

const rowId = (row: ClockAnalysisRow): string =>
  `${row.umaban}-${row.dateValue}-${row.scaledTimeTenths}`;

const clockDistanceConversion = (
  distance: number | null,
  targetDistance: number | null,
): ClockDistanceConversion => {
  if (distance === null || targetDistance === null || distance === targetDistance) {
    return "same";
  }
  return distance < targetDistance ? "shorter" : "longer";
};

// ▼ for a time scaled from a shorter race, ▲ from a longer one. Same-distance
// races keep the plain circle and need no polygon.
const markerCoordinate = (value: number): number => Number(value.toFixed(MARKER_COORDINATE_DIGITS));

export const clockMarkerTrianglePoints = ({
  conversion,
  r,
  x,
  y,
}: ClockMarkerShapeParams): string => {
  const size = r * TRIANGLE_RADIUS_SCALE;
  const halfWidth = size * TRIANGLE_HALF_WIDTH_RATIO;
  const direction = conversion === "shorter" ? -1 : 1;
  const apexY = y - direction * size;
  const baseY = y + direction * size * TRIANGLE_BASE_OFFSET_RATIO;
  return [
    `${markerCoordinate(x)},${markerCoordinate(apexY)}`,
    `${markerCoordinate(x - halfWidth)},${markerCoordinate(baseY)}`,
    `${markerCoordinate(x + halfWidth)},${markerCoordinate(baseY)}`,
  ].join(" ");
};

const toScatterPoint = (
  row: ClockAnalysisRow,
  x: number,
  y: number,
  kohanLabel: string,
  timeLabel: string,
): ClockScatterPoint => ({
  dateLabel: row.dateLabel,
  distanceConversion: row.distanceConversion,
  distanceLabel: row.distanceMeters === null ? "-" : `${row.distanceMeters}m`,
  fill: row.fill,
  horseName: row.horseName,
  id: rowId(row),
  kohanLabel,
  r: POINT_RADIUS,
  stroke: row.stroke,
  timeLabel,
  umaban: row.umaban,
  x,
  y,
});

const placedReference = ({
  kind,
  label,
  orientation,
  value,
}: ClockPlacedReferenceParams): ClockReferenceLine[] => {
  if (value === null) {
    return [];
  }
  const style = CLOCK_REFERENCE_STYLE_BY_KIND[kind];
  const horizontal = orientation === "horizontal";
  return [
    {
      kind,
      label,
      stroke: style.stroke,
      strokeDasharray: style.strokeDasharray,
      x1: horizontal ? PLOT.left : value,
      x2: horizontal ? PLOT.right : value,
      y1: horizontal ? value : PLOT.top,
      y2: horizontal ? value : PLOT.bottom,
    },
  ];
};

const optionalParValues = (...values: Array<number | null>): number[] =>
  values.flatMap((value) => (value === null ? [] : [value]));

const targetRaceKyori = (race: RaceTimeTargetRace): string | null => {
  const value = race["kyori"];
  if (typeof value !== "string") {
    return null;
  }
  return value;
};

const buildRunnerWakubanMap = (runners: ClockFrameRunner[]): Map<string, string> => {
  const map = new Map<string, string>();
  runners.forEach((runner) => {
    const ketto = cleanText(runner.sourceHorseId ?? runner.kettoTorokuBango, "");
    const wakuban = cleanText(runner.wakuban, "");
    if (ketto !== "" && wakuban !== "") {
      map.set(ketto, wakuban);
    }
  });
  return map;
};

const paintForResult = (
  result: HorseRaceResult,
  wakubanByKetto: Map<string, string>,
): HorseRaceWakubanPaint => {
  const currentWakuban = wakubanByKetto.get(cleanText(result.kettoTorokuBango, ""));
  return horseRaceChartPaintForWakuban(currentWakuban ?? result.wakuban);
};

export const buildClockAnalysisRows = ({
  includeMissingKohan,
  currentDistance,
  results,
  runners,
}: CollectAnalysisRowsParams): ClockAnalysisRow[] => {
  const targetDistance = parseRaceDistanceMeters(currentDistance);
  const wakubanByKetto = buildRunnerWakubanMap(runners);
  return results.flatMap((result) => {
    const soha = parseSohaTimeTenths(result.sohaTime);
    const kohan = parseKohan3fTenths(result.kohan3f);
    if (soha === null || (kohan === null && !includeMissingKohan)) {
      return [];
    }
    const distance = parseRaceDistanceMeters(result.kyori);
    if (targetDistance !== null && distance === null) {
      return [];
    }
    const scaled =
      targetDistance === null || distance === null
        ? soha
        : scaleSohaTimeToDistance(soha, distance, targetDistance);
    const dateValue = parseRaceChartDateValue(result.kaisaiNen, result.kaisaiTsukihi);
    if (dateValue === null) {
      return [];
    }
    const umaban = formatRunnerNumber(result.currentUmaban);
    const paint = paintForResult(result, wakubanByKetto);
    return [
      {
        dateLabel: formatDate(result.kaisaiNen, result.kaisaiTsukihi),
        dateValue,
        distanceConversion: clockDistanceConversion(distance, targetDistance),
        distanceMeters: distance,
        fill: paint.fill,
        finishRank: parseRaceFinishRank(result.kakuteiChakujun),
        horseName: cleanText(
          result.bamei ??
            runners.find(
              (runner) =>
                (runner.sourceHorseId ?? runner.kettoTorokuBango) === result.kettoTorokuBango,
            )?.bamei,
        ),
        kohan3fTenths: kohan,
        scaledTimeTenths: scaled,
        stroke: paint.outline,
        umaban,
      },
    ];
  });
};

export const buildScaledWinnerPar = ({
  currentDistance,
  races,
}: BuildScaledWinnerParParams): ClockPar => {
  const targetDistance = parseRaceDistanceMeters(currentDistance);
  const times = races.flatMap((race) => {
    const soha = parseSohaTimeTenths(race.raceTime);
    if (soha === null) {
      return [];
    }
    const distance = parseRaceDistanceMeters(targetRaceKyori(race));
    if (targetDistance === null) {
      return [soha];
    }
    if (distance === null) {
      return [];
    }
    return [scaleSohaTimeToDistance(soha, distance, targetDistance)];
  });
  const kohans = races.flatMap((race) => {
    const kohan = parseKohan3fTenths(race.kohan3f);
    return kohan === null ? [] : [kohan];
  });
  const timeSummary = summariseNumbers(times);
  const kohanSummary = summariseNumbers(kohans);
  if (timeSummary === null) {
    return EMPTY_PAR;
  }
  return {
    averageKohan3f: kohanSummary === null ? null : kohanSummary.average,
    averageRaceTime: timeSummary.average,
    fastestKohan3f: kohanSummary === null ? null : kohanSummary.fastest,
    fastestRaceTime: timeSummary.fastest,
    medianKohan3f: kohanSummary === null ? null : kohanSummary.median,
    medianRaceTime: timeSummary.median,
    sampleCount: times.length,
  };
};

export const clockParFromRaceTimeStats = (stats: RaceTimeStats | null): ClockPar => {
  if (stats === null) {
    return EMPTY_PAR;
  }
  const sampleCount =
    stats.fastestRaceTime === null &&
    stats.averageRaceTime === null &&
    stats.medianRaceTime === null &&
    stats.fastestKohan3f === null &&
    stats.averageKohan3f === null &&
    stats.medianKohan3f === null
      ? 0
      : 1;
  return {
    averageKohan3f: stats.averageKohan3f,
    averageRaceTime: stats.averageRaceTime,
    fastestKohan3f: stats.fastestKohan3f,
    fastestRaceTime: stats.fastestRaceTime,
    medianKohan3f: stats.medianKohan3f,
    medianRaceTime: stats.medianRaceTime,
    sampleCount,
  };
};

export const clockParToRaceTimeFields = (
  par: ClockPar,
): Pick<
  RaceTimeStats,
  | "averageKohan3f"
  | "averageRaceTime"
  | "fastestKohan3f"
  | "fastestRaceTime"
  | "medianKohan3f"
  | "medianRaceTime"
> => ({
  averageKohan3f: par.averageKohan3f,
  averageRaceTime: par.averageRaceTime,
  fastestKohan3f: par.fastestKohan3f,
  fastestRaceTime: par.fastestRaceTime,
  medianKohan3f: par.medianKohan3f,
  medianRaceTime: par.medianRaceTime,
});

export const withScaledWinnerClockStats = (
  stats: RaceTimeStats,
  currentDistance: string | null | undefined,
): RaceTimeStats => {
  const par = buildScaledWinnerPar({ currentDistance, races: stats.targetRaces });
  if (par.sampleCount === 0) {
    return stats;
  }
  const fields = clockParToRaceTimeFields(par);
  return {
    averageKohan3f: fields.averageKohan3f,
    averageRaceTime: fields.averageRaceTime,
    correlationRows: stats.correlationRows,
    fastestDetail: stats.fastestDetail,
    fastestKohan3f: fields.fastestKohan3f,
    fastestRaceTime: fields.fastestRaceTime,
    medianKohan3f: fields.medianKohan3f,
    medianRaceTime: fields.medianRaceTime,
    raceCount: stats.raceCount,
    targetRaces: stats.targetRaces,
  };
};

export const formatClockPointTooltip = (point: ClockScatterPoint): string[] => [
  `${point.umaban} ${point.horseName}`,
  `日付 ${point.dateLabel}`,
  point.distanceConversion === "same"
    ? `距離 ${point.distanceLabel}`
    : `距離 ${point.distanceLabel}（${CLOCK_DISTANCE_CONVERSION_LABELS[point.distanceConversion]}）`,
  `換算タイム ${point.timeLabel}`,
  `上がり3F ${point.kohanLabel}`,
];

export const formatClockReferenceTooltip = (line: ClockReferenceLine): string[] => [line.label];

export const buildScatterView = (
  rows: ClockAnalysisRow[],
  par: ClockPar,
): ClockScatterView | null => {
  if (rows.length === 0) {
    return null;
  }
  const plotted = rows
    .filter(
      (row): row is ClockAnalysisRow & { kohan3fTenths: number } => row.kohan3fTenths !== null,
    )
    .toSorted(compareClockRows);
  if (plotted.length < rows.length) return buildDateTimeView(rows);
  const xValues = [
    ...plotted.map((row) => row.kohan3fTenths),
    ...optionalParValues(par.averageKohan3f, par.fastestKohan3f, par.medianKohan3f),
  ];
  const yValues = [
    ...plotted.map((row) => row.scaledTimeTenths),
    ...optionalParValues(par.averageRaceTime, par.fastestRaceTime, par.medianRaceTime),
  ];
  const xDomain = paddedDomain(xValues);
  const yDomain = paddedDomain(yValues);
  const xScale = scaleLinear().domain([xDomain[1], xDomain[0]]).range([PLOT.left, PLOT.right]);
  const yScale = scaleLinear().domain([yDomain[1], yDomain[0]]).range([PLOT.bottom, PLOT.top]);
  const averageX = par.averageKohan3f === null ? null : xScale(par.averageKohan3f);
  const fastestX = par.fastestKohan3f === null ? null : xScale(par.fastestKohan3f);
  const medianX = par.medianKohan3f === null ? null : xScale(par.medianKohan3f);
  const averageY = par.averageRaceTime === null ? null : yScale(par.averageRaceTime);
  const fastestY = par.fastestRaceTime === null ? null : yScale(par.fastestRaceTime);
  const medianY = par.medianRaceTime === null ? null : yScale(par.medianRaceTime);
  const averageTimeLabel =
    par.averageRaceTime === null
      ? ""
      : `平均レースタイム ${formatRaceTimeTenthsLabel(par.averageRaceTime)}`;
  const fastestTimeLabel =
    par.fastestRaceTime === null
      ? ""
      : `最速レースタイム ${formatRaceTimeTenthsLabel(par.fastestRaceTime)}`;
  const medianTimeLabel =
    par.medianRaceTime === null
      ? ""
      : `中央値レースタイム ${formatRaceTimeTenthsLabel(par.medianRaceTime)}`;
  const averageKohanLabel =
    par.averageKohan3f === null
      ? ""
      : `平均上がり3F ${formatKohan3fTenthsLabel(par.averageKohan3f)}`;
  const fastestKohanLabel =
    par.fastestKohan3f === null
      ? ""
      : `最速上がり3F ${formatKohan3fTenthsLabel(par.fastestKohan3f)}`;
  const medianKohanLabel =
    par.medianKohan3f === null
      ? ""
      : `中央値上がり3F ${formatKohan3fTenthsLabel(par.medianKohan3f)}`;
  return {
    height: VIEW_HEIGHT,
    points: plotted.map((row) =>
      toScatterPoint(
        row,
        xScale(row.kohan3fTenths),
        yScale(row.scaledTimeTenths),
        formatKohan3fTenthsLabel(row.kohan3fTenths),
        formatRaceTimeTenthsLabel(row.scaledTimeTenths),
      ),
    ),
    plotBottom: PLOT.bottom,
    plotLeft: PLOT.left,
    plotRight: PLOT.right,
    plotTop: PLOT.top,
    references: [
      ...placedReference({
        kind: "fastestTime",
        label: fastestTimeLabel,
        orientation: "horizontal",
        value: fastestY,
      }),
      ...placedReference({
        kind: "averageTime",
        label: averageTimeLabel,
        orientation: "horizontal",
        value: averageY,
      }),
      ...placedReference({
        kind: "medianTime",
        label: medianTimeLabel,
        orientation: "horizontal",
        value: medianY,
      }),
      ...placedReference({
        kind: "fastestKohan",
        label: fastestKohanLabel,
        orientation: "vertical",
        value: fastestX,
      }),
      ...placedReference({
        kind: "averageKohan",
        label: averageKohanLabel,
        orientation: "vertical",
        value: averageX,
      }),
      ...placedReference({
        kind: "medianKohan",
        label: medianKohanLabel,
        orientation: "vertical",
        value: medianX,
      }),
    ],
    title: SCATTER_TITLE,
    width: VIEW_WIDTH,
    xAxisTitle: "上がり3F（右が速い）",
    xTicks: axisTicks(xScale, [xDomain[1], xDomain[0]], formatKohan3fTenthsLabel, false),
    yAxisTitle: "換算レースタイム（上が速い）",
    yTicks: axisTicks(yScale, [yDomain[1], yDomain[0]], formatRaceTimeTenthsLabel, true),
  };
};

const buildDateTimeView = (rows: ClockAnalysisRow[]): ClockScatterView => {
  const plotted = rows.toSorted(compareClockRows);
  const xDomain = paddedDomain(plotted.map((row) => row.dateValue));
  const yDomain = paddedDomain(plotted.map((row) => row.scaledTimeTenths));
  const xScale = scaleLinear().domain(xDomain).range([PLOT.left, PLOT.right]);
  const yScale = scaleLinear().domain([yDomain[1], yDomain[0]]).range([PLOT.bottom, PLOT.top]);
  return {
    height: VIEW_HEIGHT,
    width: VIEW_WIDTH,
    plotBottom: PLOT.bottom,
    plotLeft: PLOT.left,
    plotRight: PLOT.right,
    plotTop: PLOT.top,
    points: plotted.map((row) =>
      toScatterPoint(
        row,
        xScale(row.dateValue),
        yScale(row.scaledTimeTenths),
        row.kohan3fTenths === null ? "未取得" : formatKohan3fTenthsLabel(row.kohan3fTenths),
        formatRaceTimeTenthsLabel(row.scaledTimeTenths),
      ),
    ),
    references: [],
    title: "走破タイムの推移",
    xAxisTitle: "レース日",
    xTicks: axisTicks(
      xScale,
      xDomain,
      (value) => new Date(value).toISOString().slice(0, 10),
      false,
    ),
    yAxisTitle: "換算レースタイム（上が速い）",
    yTicks: axisTicks(yScale, [yDomain[1], yDomain[0]], formatRaceTimeTenthsLabel, true),
  };
};
