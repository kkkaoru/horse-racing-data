"use client";

// Run with: bunx vitest run src/app/races/detail/paddock-recent-results-chart.test.tsx
// Single-horse combined race-history chart for the paddock edit screen. It
// "踏襲"s the 競走成績グラフ by plotting one horse's 着順/人気/馬体重/馬体重増減
// in ONE ComposedChart (not split into per-metric panels).
import type { ReactElement } from "react";
import {
  Bar,
  Cell,
  ComposedChart,
  Legend,
  Line,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { formatDistance, formatKeibajo } from "../../../lib/format";
import {
  formatHorseRaceChartDate,
  getHorseRaceChartMetricValue,
  HORSE_RACE_CHART_METRIC_LABELS,
  HORSE_RACE_CHART_METRIC_UNITS,
} from "../../../lib/horse-race-results-chart-data";
import type { HorseRaceResult } from "../../../lib/race-types";
import { getFrameColor } from "./frame-number-badge";

export interface PaddockRecentResultsChartProps {
  results: HorseRaceResult[];
}

// One plottable race for the hovered horse: the X position plus the four metric
// values and the metadata the tooltip shows.
interface PaddockRecentChartRow {
  dateValue: number;
  finish: number | null;
  keibajoCode: string;
  kishumeiRyakusho: string | null;
  kyori: string | null;
  popularity: number | null;
  raceDate: string;
  wakuban: string | null;
  weight: number | null;
  weightDelta: number | null;
}

// Order key so equal-date races keep the entered-race order (raceBango asc).
interface PaddockRecentChartRowSource {
  dateValue: number;
  raceBango: string;
  row: PaddockRecentChartRow;
}

interface ChartInitialDimension {
  height: number;
  width: number;
}

interface ChartLineDot {
  r: number;
}

interface PaddockTooltipPayloadEntry {
  payload?: PaddockRecentChartRow;
}

interface PaddockTooltipProps {
  active?: boolean;
  payload?: PaddockTooltipPayloadEntry[];
}

interface PaddockTooltipMetricLineProps {
  label: string;
  unit: string;
  value: number | null;
}

interface PaddockTooltipMetaLineProps {
  label: string;
  value: string;
}

const RACE_DATE_PATTERN = /^\d{8}$/;
const RACE_DATE_YEAR_LENGTH = 4;
const RACE_DATE_MONTH_END = 6;
const RACE_DATE_DAY_END = 8;
const CHART_HEIGHT = 300;
const CHART_INITIAL_DIMENSION: ChartInitialDimension = { height: 1, width: 1 };
const CHART_LINE_DOT: ChartLineDot = { r: 2 };
const RANK_AXIS_ID = "rank";
const WEIGHT_AXIS_ID = "weight";
const DELTA_AXIS_ID = "delta";
// Rank metrics read best at the top, so their axis is reversed + anchored at 1.
const RANK_AXIS_DOMAIN: [number, "auto"] = [1, "auto"];
const VALUE_AXIS_DOMAIN: ["auto", "auto"] = ["auto", "auto"];
const TIME_AXIS_DOMAIN: ["dataMin", "dataMax"] = ["dataMin", "dataMax"];
const FINISH_LINE_COLOR = "#dc2626";
const POPULARITY_LINE_COLOR = "#2563eb";
const WEIGHT_LINE_COLOR = "#0f766e";
const DELTA_POSITIVE_COLOR = "#ea580c";
const DELTA_NEGATIVE_COLOR = "#2563eb";
const REFERENCE_LINE_COLOR = "#9ca3af";
const TOOLTIP_FRAME_FALLBACK_COLOR = "#9ca3af";
const POPULARITY_DASH = "6 3";
const REFERENCE_LINE_Y = 0;
const EMPTY_LABEL = "-";
const TOOLTIP_BORDER_WIDTH = 2;
const CHART_MARGIN = { bottom: 8, left: 8, right: 8, top: 8 };

const toRaceDate = (result: HorseRaceResult): string => result.kaisaiNen + result.kaisaiTsukihi;

const hasValidRaceDate = (result: HorseRaceResult): boolean =>
  RACE_DATE_PATTERN.test(toRaceDate(result));

const toDateValue = (raceDate: string): number =>
  Date.UTC(
    Number(raceDate.slice(0, RACE_DATE_YEAR_LENGTH)),
    Number(raceDate.slice(RACE_DATE_YEAR_LENGTH, RACE_DATE_MONTH_END)) - 1,
    Number(raceDate.slice(RACE_DATE_MONTH_END, RACE_DATE_DAY_END)),
  );

const compareRaceBango = (left: string, right: string): number => {
  if (left === right) {
    return 0;
  }
  return left < right ? -1 : 1;
};

const compareChartRowSources = (
  left: PaddockRecentChartRowSource,
  right: PaddockRecentChartRowSource,
): number => {
  if (left.dateValue !== right.dateValue) {
    return left.dateValue - right.dateValue;
  }
  return compareRaceBango(left.raceBango, right.raceBango);
};

const toChartRowSource = (result: HorseRaceResult): PaddockRecentChartRowSource => {
  const raceDate = toRaceDate(result);
  const dateValue = toDateValue(raceDate);
  return {
    dateValue,
    raceBango: result.raceBango,
    row: {
      dateValue,
      finish: getHorseRaceChartMetricValue(result, "finish"),
      keibajoCode: result.keibajoCode,
      kishumeiRyakusho: result.kishumeiRyakusho,
      kyori: result.kyori,
      popularity: getHorseRaceChartMetricValue(result, "popularity"),
      raceDate,
      wakuban: result.wakuban,
      weight: getHorseRaceChartMetricValue(result, "weight"),
      weightDelta: getHorseRaceChartMetricValue(result, "weightDelta"),
    },
  };
};

const buildChartRows = (results: HorseRaceResult[]): PaddockRecentChartRow[] =>
  results
    .filter(hasValidRaceDate)
    .map(toChartRowSource)
    .toSorted(compareChartRowSources)
    .map((source) => source.row);

const resolveDeltaColor = (delta: number): string =>
  delta >= REFERENCE_LINE_Y ? DELTA_POSITIVE_COLOR : DELTA_NEGATIVE_COLOR;

const PaddockTooltipMetricLine = ({
  label,
  unit,
  value,
}: PaddockTooltipMetricLineProps): ReactElement | null => {
  if (value === null) {
    return null;
  }
  return (
    <p className="race-results-chart-tooltip-value">
      {label} {value}
      {unit}
    </p>
  );
};

const PaddockTooltipMetaLine = ({ label, value }: PaddockTooltipMetaLineProps): ReactElement => (
  <p className="race-results-chart-tooltip-meta">
    {label} {value}
  </p>
);

const formatTooltipWakuban = (wakuban: string | null): string =>
  wakuban === null || wakuban.trim().length === 0 ? EMPTY_LABEL : wakuban;

const formatTooltipJockey = (jockey: string | null): string =>
  jockey === null || jockey.trim().length === 0 ? EMPTY_LABEL : jockey;

const PaddockRecentTooltip = ({ active, payload }: PaddockTooltipProps): ReactElement | null => {
  const entry = payload?.at(0);
  if (active !== true || entry?.payload === undefined) {
    return null;
  }
  const row = entry.payload;
  return (
    <div
      className="race-results-chart-tooltip"
      style={{
        borderColor: getFrameColor(row.wakuban) ?? TOOLTIP_FRAME_FALLBACK_COLOR,
        borderStyle: "solid",
        borderWidth: TOOLTIP_BORDER_WIDTH,
      }}
    >
      <p className="race-results-chart-tooltip-date">{formatHorseRaceChartDate(row.dateValue)}</p>
      <PaddockTooltipMetricLine
        label={HORSE_RACE_CHART_METRIC_LABELS.finish}
        unit={HORSE_RACE_CHART_METRIC_UNITS.finish}
        value={row.finish}
      />
      <PaddockTooltipMetricLine
        label={HORSE_RACE_CHART_METRIC_LABELS.popularity}
        unit={HORSE_RACE_CHART_METRIC_UNITS.popularity}
        value={row.popularity}
      />
      <PaddockTooltipMetricLine
        label={HORSE_RACE_CHART_METRIC_LABELS.weight}
        unit={HORSE_RACE_CHART_METRIC_UNITS.weight}
        value={row.weight}
      />
      <PaddockTooltipMetricLine
        label={HORSE_RACE_CHART_METRIC_LABELS.weightDelta}
        unit={HORSE_RACE_CHART_METRIC_UNITS.weightDelta}
        value={row.weightDelta}
      />
      <PaddockTooltipMetaLine label="枠番" value={formatTooltipWakuban(row.wakuban)} />
      <PaddockTooltipMetaLine label="騎手" value={formatTooltipJockey(row.kishumeiRyakusho)} />
      <PaddockTooltipMetaLine label="距離" value={formatDistance(row.kyori)} />
      <PaddockTooltipMetaLine label="競馬場" value={formatKeibajo(row.keibajoCode)} />
    </div>
  );
};

export function PaddockRecentResultsChart({
  results,
}: PaddockRecentResultsChartProps): ReactElement {
  const rows = buildChartRows(results);
  if (rows.length === 0) {
    return <p className="empty-state">表示できるレースがありません</p>;
  }
  return (
    <ResponsiveContainer
      height={CHART_HEIGHT}
      initialDimension={CHART_INITIAL_DIMENSION}
      width="100%"
    >
      <ComposedChart data={rows} margin={CHART_MARGIN}>
        <XAxis
          dataKey="dateValue"
          domain={TIME_AXIS_DOMAIN}
          scale="time"
          tickFormatter={formatHorseRaceChartDate}
          type="number"
        />
        <YAxis
          allowDecimals={false}
          domain={RANK_AXIS_DOMAIN}
          orientation="left"
          reversed
          yAxisId={RANK_AXIS_ID}
        />
        <YAxis domain={VALUE_AXIS_DOMAIN} orientation="right" yAxisId={WEIGHT_AXIS_ID} />
        <YAxis domain={VALUE_AXIS_DOMAIN} hide orientation="right" yAxisId={DELTA_AXIS_ID} />
        <Tooltip content={<PaddockRecentTooltip />} />
        <Legend />
        <ReferenceLine stroke={REFERENCE_LINE_COLOR} y={REFERENCE_LINE_Y} yAxisId={DELTA_AXIS_ID} />
        <Bar
          dataKey="weightDelta"
          name={HORSE_RACE_CHART_METRIC_LABELS.weightDelta}
          yAxisId={DELTA_AXIS_ID}
        >
          {rows.map((row) => (
            <Cell
              fill={resolveDeltaColor(row.weightDelta ?? REFERENCE_LINE_Y)}
              key={row.raceDate}
            />
          ))}
        </Bar>
        <Line
          connectNulls
          dataKey="weight"
          dot={CHART_LINE_DOT}
          isAnimationActive={false}
          name={HORSE_RACE_CHART_METRIC_LABELS.weight}
          stroke={WEIGHT_LINE_COLOR}
          type="monotone"
          yAxisId={WEIGHT_AXIS_ID}
        />
        <Line
          connectNulls
          dataKey="finish"
          dot={CHART_LINE_DOT}
          isAnimationActive={false}
          name={HORSE_RACE_CHART_METRIC_LABELS.finish}
          stroke={FINISH_LINE_COLOR}
          type="monotone"
          yAxisId={RANK_AXIS_ID}
        />
        <Line
          connectNulls
          dataKey="popularity"
          dot={CHART_LINE_DOT}
          isAnimationActive={false}
          name={HORSE_RACE_CHART_METRIC_LABELS.popularity}
          stroke={POPULARITY_LINE_COLOR}
          strokeDasharray={POPULARITY_DASH}
          type="monotone"
          yAxisId={RANK_AXIS_ID}
        />
      </ComposedChart>
    </ResponsiveContainer>
  );
}
