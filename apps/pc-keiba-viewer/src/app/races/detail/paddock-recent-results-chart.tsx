"use client";

// Run with: bunx vitest run src/app/races/detail/paddock-recent-results-chart.test.tsx
// Single-horse combined race-history chart for the paddock edit screen. It
// "踏襲"s the 競走成績グラフ by plotting one horse's 着順/人気/馬体重/馬体重増減
// in ONE ComposedChart (not split into per-metric panels). The upcoming race's
// latest weight is appended as a larger-dot point, and each metric line can be
// toggled on/off independently with its Y axis hidden when fully off.
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
  getHorseRaceChartMetricValue,
  HORSE_RACE_CHART_METRIC_LABELS,
  HORSE_RACE_CHART_METRIC_UNITS,
} from "../../../lib/horse-race-results-chart-data";
import type { HorseRaceResult } from "../../../lib/race-types";
import { getFrameColor } from "./frame-number-badge";

// Toggleable metric keys; "finish" + "popularity" share the reversed rank axis,
// "weight" owns the weight axis, "weightDelta" owns the delta axis.
type PaddockChartMetricKey = "finish" | "popularity" | "weight" | "weightDelta";

export interface PaddockRecentResultsChartProps {
  results: HorseRaceResult[];
  upcomingRaceDate?: string | null;
  upcomingWeight?: number | null;
  upcomingWeightDelta?: number | null;
}

// One plottable race for the hovered horse: the X position plus the four metric
// values and the metadata the tooltip shows. `isUpcoming` marks the synthetic
// current-race latest-weight point so it can render with a larger dot.
interface PaddockRecentChartRow {
  dateValue: number;
  finish: number | null;
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

// The resolved upcoming weight/delta + date used to seed the synthetic point.
interface PaddockUpcomingPointInput {
  raceDate: string;
  weight: number;
  weightDelta: number | null;
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
// Rank metrics read best at the top, so their axis is reversed + anchored at 1.
const RANK_AXIS_DOMAIN: [number, "auto"] = [1, "auto"];
const VALUE_AXIS_DOMAIN: ["auto", "auto"] = ["auto", "auto"];
const TIME_AXIS_DOMAIN: ["dataMin", "dataMax"] = ["dataMin", "dataMax"];
const FINISH_LINE_COLOR = "#dc2626";
const POPULARITY_LINE_COLOR = "#2563eb";
const WEIGHT_LINE_COLOR = "#0f766e";
// Orange keeps the weight-delta line non-black and distinct from the other series.
const DELTA_LINE_COLOR = "#ea580c";
const REFERENCE_LINE_COLOR = "#9ca3af";
const TOOLTIP_FRAME_FALLBACK_COLOR = "#9ca3af";
const POPULARITY_DASH = "6 3";
const REFERENCE_LINE_Y = 0;
const EMPTY_LABEL = "-";
const TOOLTIP_BORDER_WIDTH = 2;
const CHART_MARGIN = { bottom: 8, left: 8, right: 8, top: 8 };
const AXIS_TICK: ChartAxisTick = { fontSize: 11 };
const RANK_AXIS_WIDTH = 32;
const WEIGHT_AXIS_WIDTH = 40;
const DELTA_AXIS_WIDTH = 36;
const RANK_AXIS_LABEL = "着順/人気";
const WEIGHT_AXIS_LABEL = "馬体重";
const DELTA_AXIS_LABEL = "増減";
const GRID_DASH = "3 3";
const GRID_STROKE = "#e5e7eb";
const LEGEND_WRAPPER_STYLE: ChartLegendWrapperStyle = { fontSize: 12 };
const TOOLTIP_WRAPPER_STYLE: ChartTooltipWrapperStyle = { zIndex: 50 };
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
// Chip row wraps so the four toggles stay mobile-friendly at a small font size.
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

const toChartRowSource = (result: HorseRaceResult): PaddockRecentChartRowSource => {
  const raceDate = toRaceDate(result);
  const dateValue = toDateValue(raceDate);
  return {
    dateValue,
    raceBango: result.raceBango,
    row: {
      dateValue,
      finish: getHorseRaceChartMetricValue(result, "finish"),
      isUpcoming: false,
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

const UPCOMING_RACE_BANGO = "99";

// Build the synthetic upcoming-race row source: the current-race latest weight +
// delta at the target date. Rank metrics stay null so their lines do not extend.
const toUpcomingRowSource = (input: PaddockUpcomingPointInput): PaddockRecentChartRowSource => {
  const dateValue = toDateValue(input.raceDate);
  return {
    dateValue,
    raceBango: UPCOMING_RACE_BANGO,
    row: {
      dateValue,
      finish: null,
      isUpcoming: true,
      keibajoCode: "",
      kishumeiRyakusho: null,
      kyori: null,
      popularity: null,
      raceDate: input.raceDate,
      wakuban: null,
      weight: input.weight,
      weightDelta: input.weightDelta,
    },
  };
};

// Resolve the optional upcoming point: requires an 8-digit date AND a finite
// weight. A non-finite delta falls back to null so its line still connects.
const resolveUpcomingPointInput = (
  props: PaddockRecentResultsChartProps,
): PaddockUpcomingPointInput | null => {
  const raceDate = props.upcomingRaceDate ?? "";
  const weight = props.upcomingWeight;
  if (!RACE_DATE_PATTERN.test(raceDate) || typeof weight !== "number" || !Number.isFinite(weight)) {
    return null;
  }
  const delta = props.upcomingWeightDelta;
  return {
    raceDate,
    weight,
    weightDelta: typeof delta === "number" && Number.isFinite(delta) ? delta : null,
  };
};

const buildChartRows = (props: PaddockRecentResultsChartProps): PaddockRecentChartRow[] => {
  const pastSources = props.results.filter(hasValidRaceDate).map(toChartRowSource);
  const upcoming = resolveUpcomingPointInput(props);
  const allSources =
    upcoming === null ? pastSources : [...pastSources, toUpcomingRowSource(upcoming)];
  return allSources.toSorted(compareChartRowSources).map((source) => source.row);
};

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
    className="stats-control-button"
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

export function PaddockRecentResultsChart(props: PaddockRecentResultsChartProps): ReactElement {
  const [hiddenMetrics, setHiddenMetrics] = useState<ReadonlySet<PaddockChartMetricKey>>(new Set());
  const rows = buildChartRows(props);
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
      <div aria-label="近走グラフの表示系列" role="group" style={CHIP_ROW_STYLE}>
        {PADDOCK_METRIC_CHIPS.map((config) => (
          <PaddockMetricChip
            config={config}
            key={config.key}
            onToggle={toggleMetric}
            visible={!hiddenMetrics.has(config.key)}
          />
        ))}
      </div>
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
              width={RANK_AXIS_WIDTH}
              yAxisId={RANK_AXIS_ID}
            />
          ) : null}
          {hiddenMetrics.has("weight") ? null : (
            <YAxis
              domain={VALUE_AXIS_DOMAIN}
              label={WEIGHT_AXIS_LABEL}
              orientation="right"
              tick={AXIS_TICK}
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
              width={DELTA_AXIS_WIDTH}
              yAxisId={DELTA_AXIS_ID}
            />
          )}
          <Tooltip content={<PaddockRecentTooltip />} wrapperStyle={TOOLTIP_WRAPPER_STYLE} />
          <Legend wrapperStyle={LEGEND_WRAPPER_STYLE} />
          {hiddenMetrics.has("weightDelta") ? null : (
            <ReferenceLine
              stroke={REFERENCE_LINE_COLOR}
              y={REFERENCE_LINE_Y}
              yAxisId={DELTA_AXIS_ID}
            />
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
    </div>
  );
}
