"use client";

// Run with: bunx vitest run src/app/races/detail/paddock-recent-results-chart.test.tsx
// Single-horse combined race-history chart for the paddock edit screen. It
// "踏襲"s the 競走成績グラフ by plotting one horse's 着順/人気/馬体重/馬体重増減/斤量
// in ONE ComposedChart (not split into per-metric panels). The upcoming race's
// latest weight is appended as a larger-dot point, and each metric line can be
// toggled on/off independently with its Y axis hidden when fully off. A period
// slider limits the chart to the horse's most-recent N races, and the 馬体重
// line can optionally sum carried weight (斤量) when its toggle is on.
import type { CSSProperties, ReactElement } from "react";
import { useState } from "react";
import {
  CartesianGrid,
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
  getCombinedWeightValue,
  getHorseRaceChartMetricValue,
  HORSE_RACE_CHART_METRIC_LABELS,
  HORSE_RACE_CHART_METRIC_UNITS,
} from "../../../lib/horse-race-results-chart-data";
import type { HorseRaceResult } from "../../../lib/race-types";
import { getFrameColor } from "./frame-number-badge";

// Toggleable metric keys; "finish" + "popularity" share the reversed rank axis,
// "weight" owns the weight axis, "weightDelta" owns the delta axis, "futan" owns
// its own carried-weight axis (a different scale from body weight / delta).
type PaddockChartMetricKey = "finish" | "popularity" | "weight" | "weightDelta" | "futan";

export interface PaddockRecentResultsChartProps {
  results: HorseRaceResult[];
  upcomingPopularity?: number | null;
  upcomingRaceDate?: string | null;
  upcomingWeight?: number | null;
  upcomingWeightDelta?: number | null;
}

// One plottable race for the hovered horse: the X position plus the five metric
// values and the metadata the tooltip shows. `isUpcoming` marks the synthetic
// current-race latest-weight point so it can render with a larger dot.
interface PaddockRecentChartRow {
  dateValue: number;
  finish: number | null;
  futan: number | null;
  isUpcoming: boolean;
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

// The resolved upcoming weight/delta/popularity + date used to seed the
// synthetic point. Each metric is independent: any of them may be null while
// the row is still emitted as long as at least one resolved value remains.
interface PaddockUpcomingPointInput {
  popularity: number | null;
  raceDate: string;
  weight: number | null;
  weightDelta: number | null;
}

// State threaded into buildChartRows: the combine-futan toggle plus the
// resolved most-recent-N race window from the period slider.
interface PaddockChartBuildOptions {
  combineWeightFutan: boolean;
  recentCount: number;
}

interface ChartInitialDimension {
  height: number;
  width: number;
}

interface ChartLineDot {
  r: number;
}

interface ChartAxisTick {
  fontSize: number;
}

interface ChartLegendWrapperStyle {
  fontSize: number;
}

interface ChartTooltipWrapperStyle {
  zIndex: number;
}

// One per-metric toggle chip: its key, Japanese label, swatch color, and the
// axis it belongs to so the axis can hide when every metric on it is off.
interface PaddockMetricChipConfig {
  axisId: string;
  color: string;
  dash: string | null;
  key: PaddockChartMetricKey;
  label: string;
}

interface PaddockChartDotPayload {
  isUpcoming?: boolean;
}

interface PaddockChartDotProps {
  cx?: number;
  cy?: number;
  payload?: PaddockChartDotPayload;
  stroke?: string;
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
const CHART_HEIGHT = 340;
const CHART_INITIAL_DIMENSION: ChartInitialDimension = { height: 1, width: 1 };
const CHART_LINE_DOT: ChartLineDot = { r: 2 };
const UPCOMING_DOT_RADIUS = 4;
const RANK_AXIS_ID = "rank";
const WEIGHT_AXIS_ID = "weight";
const DELTA_AXIS_ID = "delta";
const FUTAN_AXIS_ID = "futan";
// Rank metrics read best at the top, so their axis is reversed + anchored at 1.
const RANK_AXIS_DOMAIN: [number, "auto"] = [1, "auto"];
const VALUE_AXIS_DOMAIN: ["auto", "auto"] = ["auto", "auto"];
const TIME_AXIS_DOMAIN: ["dataMin", "dataMax"] = ["dataMin", "dataMax"];
// Unified metric palette shared with the 競走成績グラフ 馬別相関 mode (req 4): a
// clearly-distinguishable, saturated set that stays readable on the white chart
// background. Hues are well separated — crimson, blue, emerald, amber, violet —
// with no near-duplicate pairs.
const FINISH_LINE_COLOR = "#e03131"; // crimson red — 着順
const POPULARITY_LINE_COLOR = "#1971c2"; // blue — 人気
const WEIGHT_LINE_COLOR = "#0ca678"; // emerald green — 馬体重
const DELTA_LINE_COLOR = "#f59f00"; // amber — 馬体重増減
const FUTAN_LINE_COLOR = "#7048e8"; // violet — 斤量
const REFERENCE_LINE_COLOR = "#adb5bd";
const TOOLTIP_FRAME_FALLBACK_COLOR = "#adb5bd";
const POPULARITY_DASH = "6 3";
const FUTAN_DASH = "4 4";
const REFERENCE_LINE_Y = 0;
// Popularity ranks start at 1 (favourite); anything below is treated as unknown.
const MIN_POPULARITY_RANK = 1;
const EMPTY_LABEL = "-";
const TOOLTIP_BORDER_WIDTH = 2;
const CHART_MARGIN = { bottom: 8, left: 12, right: 12, top: 8 };
const AXIS_TICK: ChartAxisTick = { fontSize: 11 };
// Wider axes prevent multi-digit tick labels (weights ~480, futan ~57) from
// clipping; the rank axis needs less because ranks are 1-2 digits (#12).
const RANK_AXIS_WIDTH = 36;
const WEIGHT_AXIS_WIDTH = 48;
const DELTA_AXIS_WIDTH = 44;
const FUTAN_AXIS_WIDTH = 44;
// Cap dense axes so ticks do not overlap on a narrow mobile chart (#12).
const RANK_AXIS_TICK_COUNT = 6;
const VALUE_AXIS_TICK_COUNT = 6;
const RANK_AXIS_LABEL = "着順/人気";
const WEIGHT_AXIS_LABEL = "馬体重";
const WEIGHT_FUTAN_AXIS_LABEL = "馬体重+斤量";
const DELTA_AXIS_LABEL = "増減";
const FUTAN_AXIS_LABEL = "斤量";
const GRID_DASH = "3 3";
const GRID_STROKE = "#e9ecef";
const LEGEND_WRAPPER_STYLE: ChartLegendWrapperStyle = { fontSize: 12 };
const TOOLTIP_WRAPPER_STYLE: ChartTooltipWrapperStyle = { zIndex: 50 };
// futan defaults to hidden to keep the initial chart uncluttered (TASK 1).
const INITIAL_HIDDEN_METRICS: ReadonlySet<PaddockChartMetricKey> = new Set<PaddockChartMetricKey>([
  "futan",
]);
const COMBINE_WEIGHT_FUTAN_DEFAULT = false;
// Period slider bounds: never fewer than 3 races, default to the most recent 10.
const PERIOD_MIN_COUNT = 3;
const PERIOD_DEFAULT_COUNT = 10;
const PERIOD_STEP = 1;
const COMBINE_TOGGLE_LABEL = "馬体重に斤量を合算";
// Inline tooltip styling keeps it readable on mobile (background + shadow + wrap).
const TOOLTIP_BASE_STYLE: CSSProperties = {
  backgroundColor: "#ffffff",
  borderRadius: 6,
  borderStyle: "solid",
  borderWidth: TOOLTIP_BORDER_WIDTH,
  boxShadow: "0 2px 8px rgba(0, 0, 0, 0.18)",
  color: "#1f2937",
  fontSize: 13,
  lineHeight: 1.5,
  maxWidth: 220,
  padding: "8px 10px",
};
// Chip row wraps so the toggles stay mobile-friendly at a small font size.
const CHIP_ROW_STYLE: CSSProperties = {
  display: "flex",
  flexWrap: "wrap",
  fontSize: 12,
  gap: 6,
  marginBottom: 6,
};
const CHIP_STYLE: CSSProperties = {
  alignItems: "center",
  display: "inline-flex",
  fontSize: 12,
  gap: 4,
};
const CHIP_SWATCH_BASE_STYLE: CSSProperties = {
  borderRadius: 2,
  display: "inline-block",
  height: 10,
  width: 14,
};
// Toggle chips, in render order, each tied to its line color + owning axis.
const PADDOCK_METRIC_CHIPS: PaddockMetricChipConfig[] = [
  {
    axisId: RANK_AXIS_ID,
    color: FINISH_LINE_COLOR,
    dash: null,
    key: "finish",
    label: HORSE_RACE_CHART_METRIC_LABELS.finish,
  },
  {
    axisId: RANK_AXIS_ID,
    color: POPULARITY_LINE_COLOR,
    dash: POPULARITY_DASH,
    key: "popularity",
    label: HORSE_RACE_CHART_METRIC_LABELS.popularity,
  },
  {
    axisId: WEIGHT_AXIS_ID,
    color: WEIGHT_LINE_COLOR,
    dash: null,
    key: "weight",
    label: HORSE_RACE_CHART_METRIC_LABELS.weight,
  },
  {
    axisId: DELTA_AXIS_ID,
    color: DELTA_LINE_COLOR,
    dash: null,
    key: "weightDelta",
    label: HORSE_RACE_CHART_METRIC_LABELS.weightDelta,
  },
  {
    axisId: FUTAN_AXIS_ID,
    color: FUTAN_LINE_COLOR,
    dash: FUTAN_DASH,
    key: "futan",
    label: HORSE_RACE_CHART_METRIC_LABELS.futan,
  },
];

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

const toChartRowSource = (
  result: HorseRaceResult,
  combineWeightFutan: boolean,
): PaddockRecentChartRowSource => {
  const raceDate = toRaceDate(result);
  const dateValue = toDateValue(raceDate);
  return {
    dateValue,
    raceBango: result.raceBango,
    row: {
      dateValue,
      finish: getHorseRaceChartMetricValue(result, "finish"),
      futan: getHorseRaceChartMetricValue(result, "futan"),
      isUpcoming: false,
      keibajoCode: result.keibajoCode,
      kishumeiRyakusho: result.kishumeiRyakusho,
      kyori: result.kyori,
      popularity: getHorseRaceChartMetricValue(result, "popularity"),
      raceDate,
      wakuban: result.wakuban,
      weight: getCombinedWeightValue(result, combineWeightFutan),
      weightDelta: getHorseRaceChartMetricValue(result, "weightDelta"),
    },
  };
};

const UPCOMING_RACE_BANGO = "99";

// Build the synthetic upcoming-race row source: the current-race latest weight +
// delta + target-race popularity at the target date. Each metric is filled
// independently (any may be null), so the popularity line can reach the upcoming
// point even with no weight snapshot yet. finish/futan stay null so those lines
// do not extend past the latest result (no result yet for the upcoming race).
const toUpcomingRowSource = (input: PaddockUpcomingPointInput): PaddockRecentChartRowSource => {
  const dateValue = toDateValue(input.raceDate);
  return {
    dateValue,
    raceBango: UPCOMING_RACE_BANGO,
    row: {
      dateValue,
      finish: null,
      futan: null,
      isUpcoming: true,
      keibajoCode: "",
      kishumeiRyakusho: null,
      kyori: null,
      popularity: input.popularity,
      raceDate: input.raceDate,
      wakuban: null,
      weight: input.weight,
      weightDelta: input.weightDelta,
    },
  };
};

// Resolve the upcoming target-race popularity: a finite rank >= 1 becomes the
// point value, anything else falls back to null so the line stops at the latest
// past race rather than plotting an invalid rank.
const resolveUpcomingPopularity = (popularity: number | null | undefined): number | null =>
  typeof popularity === "number" && Number.isFinite(popularity) && popularity >= MIN_POPULARITY_RANK
    ? popularity
    : null;

// Resolve an optional finite metric value: anything non-finite becomes null so
// the metric is simply absent from the upcoming row rather than plotting garbage.
const resolveFiniteMetric = (value: number | null | undefined): number | null =>
  typeof value === "number" && Number.isFinite(value) ? value : null;

// Resolve the optional upcoming point: requires an 8-digit date AND at least one
// resolved metric among weight, weightDelta or popularity. Each metric is filled
// independently so e.g. the popularity line can reach the upcoming point even
// when no weight snapshot exists yet (odds live before the weigh-in).
const resolveUpcomingPointInput = (
  props: PaddockRecentResultsChartProps,
): PaddockUpcomingPointInput | null => {
  const raceDate = props.upcomingRaceDate ?? "";
  if (!RACE_DATE_PATTERN.test(raceDate)) {
    return null;
  }
  const weight = resolveFiniteMetric(props.upcomingWeight);
  const weightDelta = resolveFiniteMetric(props.upcomingWeightDelta);
  const popularity = resolveUpcomingPopularity(props.upcomingPopularity);
  if (weight === null && weightDelta === null && popularity === null) {
    return null;
  }
  return { popularity, raceDate, weight, weightDelta };
};

// Keep only the most-recent N past sources by date (ties resolved by raceBango),
// preserving ascending order. When N covers every race the list is unchanged.
const limitToRecentSources = (
  sources: PaddockRecentChartRowSource[],
  recentCount: number,
): PaddockRecentChartRowSource[] => {
  const ascending = sources.toSorted(compareChartRowSources);
  return ascending.slice(Math.max(0, ascending.length - recentCount));
};

const buildChartRows = (
  props: PaddockRecentResultsChartProps,
  options: PaddockChartBuildOptions,
): PaddockRecentChartRow[] => {
  const pastSources = props.results
    .filter(hasValidRaceDate)
    .map((result) => toChartRowSource(result, options.combineWeightFutan));
  const recentSources = limitToRecentSources(pastSources, options.recentCount);
  const upcoming = resolveUpcomingPointInput(props);
  const allSources =
    upcoming === null ? recentSources : [...recentSources, toUpcomingRowSource(upcoming)];
  return allSources.toSorted(compareChartRowSources).map((source) => source.row);
};

// Total count of past races with a valid date; the period slider's maximum.
const countValidResults = (props: PaddockRecentResultsChartProps): number =>
  props.results.filter(hasValidRaceDate).length;

// The default resolved window: the most recent 10 races, or fewer when the horse
// has run fewer than 10 (never below the slider minimum of 3).
const resolveDefaultRecentCount = (total: number): number =>
  Math.max(PERIOD_MIN_COUNT, Math.min(PERIOD_DEFAULT_COUNT, total));

export const PaddockChartDot = ({
  cx,
  cy,
  payload,
  stroke,
}: PaddockChartDotProps): ReactElement | null => {
  if (cx === undefined || cy === undefined) {
    return null;
  }
  const radius = payload?.isUpcoming === true ? UPCOMING_DOT_RADIUS : CHART_LINE_DOT.r;
  return <circle cx={cx} cy={cy} fill={stroke} r={radius} stroke={stroke} />;
};

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

export const PaddockRecentTooltip = ({
  active,
  payload,
}: PaddockTooltipProps): ReactElement | null => {
  const entry = payload?.at(0);
  if (active !== true || entry?.payload === undefined) {
    return null;
  }
  const row = entry.payload;
  return (
    <div
      className="race-results-chart-tooltip"
      style={{
        ...TOOLTIP_BASE_STYLE,
        borderColor: getFrameColor(row.wakuban) ?? TOOLTIP_FRAME_FALLBACK_COLOR,
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
      <PaddockTooltipMetricLine
        label={HORSE_RACE_CHART_METRIC_LABELS.futan}
        unit={HORSE_RACE_CHART_METRIC_UNITS.futan}
        value={row.futan}
      />
      <PaddockTooltipMetaLine label="枠番" value={formatTooltipWakuban(row.wakuban)} />
      <PaddockTooltipMetaLine label="騎手" value={formatTooltipJockey(row.kishumeiRyakusho)} />
      <PaddockTooltipMetaLine label="距離" value={formatDistance(row.kyori)} />
      <PaddockTooltipMetaLine label="競馬場" value={formatKeibajo(row.keibajoCode)} />
    </div>
  );
};

interface PaddockMetricChipProps {
  config: PaddockMetricChipConfig;
  onToggle: (key: PaddockChartMetricKey) => void;
  visible: boolean;
}

const PaddockMetricChip = ({ config, onToggle, visible }: PaddockMetricChipProps): ReactElement => (
  <button
    aria-pressed={visible}
    className="stats-control-button race-results-chart-chip"
    data-active={visible}
    style={CHIP_STYLE}
    type="button"
    onClick={() => {
      onToggle(config.key);
    }}
  >
    <span aria-hidden="true" style={{ ...CHIP_SWATCH_BASE_STYLE, backgroundColor: config.color }} />
    {config.label}
  </button>
);

// A metric line renders only when its chip is visible; the upcoming point uses
// the larger dot via the shared custom-dot renderer.
interface PaddockMetricLineProps {
  axisId: string;
  color: string;
  dataKey: PaddockChartMetricKey;
  dash: string | null;
  label: string;
}

const PaddockMetricLine = ({
  axisId,
  color,
  dataKey,
  dash,
  label,
}: PaddockMetricLineProps): ReactElement => (
  <Line
    connectNulls
    dataKey={dataKey}
    dot={<PaddockChartDot />}
    isAnimationActive={false}
    name={label}
    stroke={color}
    strokeDasharray={dash ?? undefined}
    type="monotone"
    yAxisId={axisId}
  />
);

const isRankAxisVisible = (hidden: ReadonlySet<PaddockChartMetricKey>): boolean =>
  !hidden.has("finish") || !hidden.has("popularity");

// The weight axis label reflects whether carried weight is being summed (#10).
const resolveWeightAxisLabel = (combineWeightFutan: boolean): string =>
  combineWeightFutan ? WEIGHT_FUTAN_AXIS_LABEL : WEIGHT_AXIS_LABEL;

const resolveWeightChipLabel = (combineWeightFutan: boolean): string =>
  combineWeightFutan ? WEIGHT_FUTAN_AXIS_LABEL : HORSE_RACE_CHART_METRIC_LABELS.weight;

// Label shows the resolved window: 全◯走 when N covers every race, else 直近N走.
const resolvePeriodLabel = (recentCount: number, total: number): string =>
  recentCount >= total ? `全${total}走` : `直近${recentCount}走`;

interface PaddockPeriodSliderProps {
  onChange: (count: number) => void;
  recentCount: number;
  total: number;
}

const PaddockPeriodSlider = ({
  onChange,
  recentCount,
  total,
}: PaddockPeriodSliderProps): ReactElement => (
  <div className="paddock-recent-chart-period">
    <label className="paddock-recent-chart-period-label">
      <span className="paddock-recent-chart-period-text">
        {resolvePeriodLabel(recentCount, total)}
      </span>
      <input
        className="paddock-recent-chart-period-input"
        max={total}
        min={PERIOD_MIN_COUNT}
        step={PERIOD_STEP}
        type="range"
        value={recentCount}
        onChange={(event) => {
          onChange(Number(event.target.value));
        }}
      />
    </label>
  </div>
);

interface PaddockCombineToggleProps {
  combineWeightFutan: boolean;
  onToggle: () => void;
}

const PaddockCombineToggle = ({
  combineWeightFutan,
  onToggle,
}: PaddockCombineToggleProps): ReactElement => (
  <button
    aria-pressed={combineWeightFutan}
    className="stats-control-button race-results-chart-chip"
    data-active={combineWeightFutan}
    style={CHIP_STYLE}
    type="button"
    onClick={onToggle}
  >
    {COMBINE_TOGGLE_LABEL}
  </button>
);

interface PaddockChartCanvasProps {
  combineWeightFutan: boolean;
  hiddenMetrics: ReadonlySet<PaddockChartMetricKey>;
  rows: PaddockRecentChartRow[];
}

const PaddockChartCanvas = ({
  combineWeightFutan,
  hiddenMetrics,
  rows,
}: PaddockChartCanvasProps): ReactElement => (
  <ResponsiveContainer
    height={CHART_HEIGHT}
    initialDimension={CHART_INITIAL_DIMENSION}
    width="100%"
  >
    <ComposedChart data={rows} margin={CHART_MARGIN}>
      <CartesianGrid stroke={GRID_STROKE} strokeDasharray={GRID_DASH} />
      <XAxis
        dataKey="dateValue"
        domain={TIME_AXIS_DOMAIN}
        scale="time"
        tick={AXIS_TICK}
        tickFormatter={formatHorseRaceChartDate}
        type="number"
      />
      {isRankAxisVisible(hiddenMetrics) ? (
        <YAxis
          allowDecimals={false}
          domain={RANK_AXIS_DOMAIN}
          label={RANK_AXIS_LABEL}
          orientation="left"
          reversed
          tick={AXIS_TICK}
          tickCount={RANK_AXIS_TICK_COUNT}
          width={RANK_AXIS_WIDTH}
          yAxisId={RANK_AXIS_ID}
        />
      ) : null}
      {hiddenMetrics.has("weight") ? null : (
        <YAxis
          domain={VALUE_AXIS_DOMAIN}
          label={resolveWeightAxisLabel(combineWeightFutan)}
          orientation="right"
          tick={AXIS_TICK}
          tickCount={VALUE_AXIS_TICK_COUNT}
          width={WEIGHT_AXIS_WIDTH}
          yAxisId={WEIGHT_AXIS_ID}
        />
      )}
      {hiddenMetrics.has("weightDelta") ? null : (
        <YAxis
          allowDecimals={false}
          domain={VALUE_AXIS_DOMAIN}
          label={DELTA_AXIS_LABEL}
          orientation="right"
          tick={AXIS_TICK}
          tickCount={VALUE_AXIS_TICK_COUNT}
          width={DELTA_AXIS_WIDTH}
          yAxisId={DELTA_AXIS_ID}
        />
      )}
      {hiddenMetrics.has("futan") ? null : (
        <YAxis
          allowDecimals={false}
          domain={VALUE_AXIS_DOMAIN}
          label={FUTAN_AXIS_LABEL}
          orientation="right"
          tick={AXIS_TICK}
          tickCount={VALUE_AXIS_TICK_COUNT}
          width={FUTAN_AXIS_WIDTH}
          yAxisId={FUTAN_AXIS_ID}
        />
      )}
      <Tooltip content={<PaddockRecentTooltip />} wrapperStyle={TOOLTIP_WRAPPER_STYLE} />
      <Legend wrapperStyle={LEGEND_WRAPPER_STYLE} />
      {hiddenMetrics.has("weightDelta") ? null : (
        <ReferenceLine stroke={REFERENCE_LINE_COLOR} y={REFERENCE_LINE_Y} yAxisId={DELTA_AXIS_ID} />
      )}
      {PADDOCK_METRIC_CHIPS.map((config) =>
        hiddenMetrics.has(config.key) ? null : (
          <PaddockMetricLine
            axisId={config.axisId}
            color={config.color}
            dataKey={config.key}
            dash={config.dash}
            key={config.key}
            label={config.label}
          />
        ),
      )}
    </ComposedChart>
  </ResponsiveContainer>
);

export function PaddockRecentResultsChart(props: PaddockRecentResultsChartProps): ReactElement {
  const total = countValidResults(props);
  const [hiddenMetrics, setHiddenMetrics] =
    useState<ReadonlySet<PaddockChartMetricKey>>(INITIAL_HIDDEN_METRICS);
  const [combineWeightFutan, setCombineWeightFutan] = useState(COMBINE_WEIGHT_FUTAN_DEFAULT);
  const [recentCount, setRecentCount] = useState(resolveDefaultRecentCount(total));
  const rows = buildChartRows(props, { combineWeightFutan, recentCount });
  const toggleMetric = (key: PaddockChartMetricKey): void => {
    setHiddenMetrics((current) => {
      const next = new Set(current);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  };
  if (rows.length === 0) {
    return <p className="empty-state">表示できるレースがありません</p>;
  }
  return (
    <div>
      <PaddockPeriodSlider onChange={setRecentCount} recentCount={recentCount} total={total} />
      <fieldset
        aria-label="近走グラフの表示系列"
        className="paddock-chip-row"
        style={CHIP_ROW_STYLE}
      >
        {PADDOCK_METRIC_CHIPS.map((config) => (
          <PaddockMetricChip
            config={
              config.key === "weight"
                ? { ...config, label: resolveWeightChipLabel(combineWeightFutan) }
                : config
            }
            key={config.key}
            onToggle={toggleMetric}
            visible={!hiddenMetrics.has(config.key)}
          />
        ))}
        <PaddockCombineToggle
          combineWeightFutan={combineWeightFutan}
          onToggle={() => {
            setCombineWeightFutan((current) => !current);
          }}
        />
      </fieldset>
      <PaddockChartCanvas
        combineWeightFutan={combineWeightFutan}
        hiddenMetrics={hiddenMetrics}
        rows={rows}
      />
    </div>
  );
}
