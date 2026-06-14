"use client";

import { useMemo, useState } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { formatDistance } from "../../../lib/format";
import {
  buildHorseRaceChartSeriesList,
  buildHorseRaceCorrelationRows,
  filterHorseRaceResultsToRecentYears,
  formatHorseRaceChartDate,
  HORSE_RACE_CHART_METRIC_LABELS,
  HORSE_RACE_CHART_METRIC_UNITS,
  HORSE_RACE_CHART_METRICS,
} from "../../../lib/horse-race-results-chart-data";
import type {
  HorseRaceChartMetric,
  HorseRaceChartPoint,
  HorseRaceChartRunner,
  HorseRaceChartSeries,
  UpcomingWeightOverride,
} from "../../../lib/horse-race-results-chart-data";
import { useHorseWeightStream } from "../../../lib/horse-weight-stream-client";
import type { HorseWeightEntry } from "../../../lib/horse-weight-stream-client";
import type { HorseRaceResult } from "../../../lib/race-types";
import { FRAME_COLORS, getFrameColor } from "./frame-number-badge";
import { HorseRaceResultsCorrelationMatrix } from "./horse-race-results-correlation-matrix";

type ChartViewMode = "overview" | "correlation";

interface HorseRaceResultsChartProps {
  day?: string;
  keibajoCode?: string;
  month?: string;
  raceNumber?: string;
  results: HorseRaceResult[];
  runners?: HorseRaceChartRunner[];
  source?: string;
  targetKeibajoCode?: string | null;
  targetRaceDate?: string | null;
  year?: string;
}

interface ChartInitialDimension {
  height: number;
  width: number;
}

interface ChartLineDot {
  r: number;
}

interface ChartTooltipPayloadEntry {
  payload?: HorseRaceChartPoint;
  value?: number | string;
}

interface MetricTooltipProps {
  active?: boolean;
  metric: HorseRaceChartMetric;
  payload?: ChartTooltipPayloadEntry[];
}

interface OverviewPanelsProps {
  hiddenHorses: ReadonlySet<string>;
  seriesListsByMetric: Record<HorseRaceChartMetric, HorseRaceChartSeries[]>;
}

interface SeriesListsInput {
  results: HorseRaceResult[];
  runners: HorseRaceChartRunner[] | undefined;
  targetKeibajoCode: string | null | undefined;
  targetRaceDate: string | null | undefined;
  upcomingWeights: UpcomingWeightOverride[];
}

type ChartYAxisDomain = [number, "auto"] | ["auto", "auto"];

const CHART_WINDOW_YEARS = 3;
// Realtime weight change sign that flips the (already decoded) change amount
// negative; matches HorseWeightEntry.changeSign emitted by the weight stream.
const WEIGHT_NEGATIVE_SIGN = "-";
const CHART_PANEL_HEIGHT = 260;
const CHART_GRID_STROKE = "#d8e0da";
const CHART_GRID_DASH = "3 3";
// Match オッズ推移: a normal series uses 2.4 and a white frame is boosted to
// 3.2 so its white line stays visible on the white chart background.
const CHART_LINE_STROKE_WIDTH = 2.4;
const WHITE_FRAME_LINE_STROKE_WIDTH = 3.2;
const WHITE_FRAME_HEX = "#ffffff";
// Frames whose badge color is white, derived from the shared FRAME_COLORS table
// (the same source オッズ推移 uses) so the white-frame stroke boost stays
// data-driven instead of relying on a magic frame number.
const WHITE_FRAME_NUMBERS: ReadonlySet<string> = new Set(
  Object.keys(FRAME_COLORS).filter((frame) => FRAME_COLORS[frame] === WHITE_FRAME_HEX),
);
const CHART_INITIAL_DIMENSION: ChartInitialDimension = { height: 1, width: 1 };
const CHART_LINE_DOT: ChartLineDot = { r: 2 };
// The four overview panels share one syncId and match hover points by X value,
// so hovering one date highlights the same race date in every panel.
const OVERVIEW_SYNC_ID = "race-results-overview";
const CHART_SYNC_METHOD = "value";
const CHART_VIEW_MODES: ChartViewMode[] = ["overview", "correlation"];
const CHART_VIEW_MODE_LABELS: Record<ChartViewMode, string> = {
  correlation: "馬別（相関）",
  overview: "俯瞰（指標別）",
};
const CHIP_GROUP_LABELS: Record<ChartViewMode, string> = {
  correlation: "相関を表示する馬の選択",
  overview: "馬ごとの表示切替",
};
// Rank metrics (finish / popularity) read best at the top, so their Y axis is
// reversed and anchored at rank 1; value metrics use a plain auto domain.
const RANK_AXIS_DOMAIN: [number, "auto"] = [1, "auto"];
const VALUE_AXIS_DOMAIN: ["auto", "auto"] = ["auto", "auto"];
const TIME_AXIS_DOMAIN: ["dataMin", "dataMax"] = ["dataMin", "dataMax"];
const REVERSED_Y_AXIS_BY_METRIC: Record<HorseRaceChartMetric, boolean> = {
  finish: true,
  popularity: true,
  weight: false,
  weightDelta: false,
};
const Y_AXIS_DOMAIN_BY_METRIC: Record<HorseRaceChartMetric, ChartYAxisDomain> = {
  finish: RANK_AXIS_DOMAIN,
  popularity: RANK_AXIS_DOMAIN,
  weight: VALUE_AXIS_DOMAIN,
  weightDelta: VALUE_AXIS_DOMAIN,
};
// Only the rank panels carry per-race distance + jockey on their points, so only
// they get the custom tooltip; weight/delta panels keep the default tooltip.
const METRIC_HAS_RACE_CONTEXT: Record<HorseRaceChartMetric, boolean> = {
  finish: true,
  popularity: true,
  weight: false,
  weightDelta: false,
};

const getHorseChipLabel = (series: HorseRaceChartSeries): string =>
  `${series.umaban ?? "-"} ${series.bamei}`;

// Color = オッズ推移: the entered-race frame color (resolved from series.frame)
// takes precedence so a horse's chart line matches its odds-trend line; the
// palette color stays only as a fallback when the horse has no frame.
const resolveSeriesStroke = (series: HorseRaceChartSeries): string =>
  getFrameColor(series.frame) ?? series.color;

const resolveSeriesStrokeWidth = (series: HorseRaceChartSeries): number =>
  series.frame !== null && WHITE_FRAME_NUMBERS.has(series.frame)
    ? WHITE_FRAME_LINE_STROKE_WIDTH
    : CHART_LINE_STROKE_WIDTH;

// Horse grouping is metric-independent, so every list shares the same horses
// in the same order; only the plotted points differ between metrics.
const buildSeriesListsByMetric = ({
  results,
  runners,
  targetKeibajoCode,
  targetRaceDate,
  upcomingWeights,
}: SeriesListsInput): Record<HorseRaceChartMetric, HorseRaceChartSeries[]> => ({
  finish: buildHorseRaceChartSeriesList({ metric: "finish", results, runners }),
  popularity: buildHorseRaceChartSeriesList({ metric: "popularity", results, runners }),
  weight: buildHorseRaceChartSeriesList({
    metric: "weight",
    results,
    runners,
    targetKeibajoCode,
    targetRaceDate,
    upcomingWeights,
  }),
  weightDelta: buildHorseRaceChartSeriesList({
    metric: "weightDelta",
    results,
    runners,
    targetKeibajoCode,
    targetRaceDate,
    upcomingWeights,
  }),
});

// Apply the realtime change sign to the (already decoded) change amount so the
// delta carries its direction: a "-" sign negates the amount, anything else
// keeps it positive. Null when no change amount is available.
const signedWeightDelta = (entry: HorseWeightEntry): number | null =>
  entry.changeAmount === null
    ? null
    : entry.changeSign === WEIGHT_NEGATIVE_SIGN
      ? -entry.changeAmount
      : entry.changeAmount;

// Convert one realtime weight snapshot into the numeric upcoming-weight overrides
// the chart-data lib consumes. The weight + changeAmount arrive already decoded
// (kg); the delta sign is applied here so the lib never re-parses strings.
const toUpcomingWeightOverrides = (horses: HorseWeightEntry[]): UpcomingWeightOverride[] =>
  horses.map((horse) => ({
    umaban: horse.horseNumber,
    weight: horse.weight,
    weightDelta: signedWeightDelta(horse),
  }));

const MetricTooltip = ({ active, metric, payload }: MetricTooltipProps) => {
  const entry = payload?.at(0);
  if (active !== true || !entry?.payload) {
    return null;
  }
  const point = entry.payload;
  return (
    <div className="race-results-chart-tooltip">
      <p className="race-results-chart-tooltip-date">{formatHorseRaceChartDate(point.dateValue)}</p>
      <p className="race-results-chart-tooltip-value">
        {String(entry.value ?? point.value)}
        {HORSE_RACE_CHART_METRIC_UNITS[metric]}
      </p>
      <p className="race-results-chart-tooltip-meta">距離 {formatDistance(point.kyori)}</p>
      <p className="race-results-chart-tooltip-meta">騎手 {point.jockey ?? "-"}</p>
    </div>
  );
};

const OverviewPanels = ({ hiddenHorses, seriesListsByMetric }: OverviewPanelsProps) => (
  <div className="race-results-chart-grid">
    {HORSE_RACE_CHART_METRICS.map((metric) => (
      <section className="race-results-chart-panel" key={metric}>
        <h3>{HORSE_RACE_CHART_METRIC_LABELS[metric]}</h3>
        <ResponsiveContainer
          height={CHART_PANEL_HEIGHT}
          initialDimension={CHART_INITIAL_DIMENSION}
          width="100%"
        >
          <LineChart syncId={OVERVIEW_SYNC_ID} syncMethod={CHART_SYNC_METHOD}>
            <CartesianGrid stroke={CHART_GRID_STROKE} strokeDasharray={CHART_GRID_DASH} />
            <XAxis
              dataKey="dateValue"
              domain={TIME_AXIS_DOMAIN}
              scale="time"
              tickFormatter={formatHorseRaceChartDate}
              type="number"
            />
            <YAxis
              allowDecimals={false}
              domain={Y_AXIS_DOMAIN_BY_METRIC[metric]}
              reversed={REVERSED_Y_AXIS_BY_METRIC[metric]}
            />
            {METRIC_HAS_RACE_CONTEXT[metric] ? (
              <Tooltip content={<MetricTooltip metric={metric} />} />
            ) : (
              <Tooltip
                formatter={(value) => `${String(value)}${HORSE_RACE_CHART_METRIC_UNITS[metric]}`}
                labelFormatter={(label) => formatHorseRaceChartDate(Number(label))}
              />
            )}
            {seriesListsByMetric[metric]
              .filter((series) => !hiddenHorses.has(series.kettoTorokuBango))
              .map((series) => (
                <Line
                  data={series.points}
                  dataKey="value"
                  dot={CHART_LINE_DOT}
                  isAnimationActive={false}
                  key={series.kettoTorokuBango}
                  name={getHorseChipLabel(series)}
                  stroke={resolveSeriesStroke(series)}
                  strokeWidth={resolveSeriesStrokeWidth(series)}
                  type="monotone"
                />
              ))}
          </LineChart>
        </ResponsiveContainer>
      </section>
    ))}
  </div>
);

export const HorseRaceResultsChart = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  results,
  runners,
  source,
  targetKeibajoCode,
  targetRaceDate,
  year,
}: HorseRaceResultsChartProps) => {
  const [viewMode, setViewMode] = useState<ChartViewMode>("overview");
  const [hiddenHorses, setHiddenHorses] = useState<ReadonlySet<string>>(new Set<string>());
  const [selectedHorse, setSelectedHorse] = useState<string | null>(null);
  // Mirror runners-table: subscribe to the realtime 馬体重 stream so the upcoming
  // weight/delta come from the live snapshot (the static results payload leaves
  // them blank). `initial: null` lets the hook self-fetch via SSE on mount, the
  // same source the runners table resolves weight from in dev.
  const horseWeightSnapshot = useHorseWeightStream({
    day: day ?? "",
    initial: null,
    keibajoCode: keibajoCode ?? "",
    month: month ?? "",
    raceNumber: raceNumber ?? "",
    source: source ?? "",
    year: year ?? "",
  });
  const upcomingWeights = useMemo(
    () => toUpcomingWeightOverrides(horseWeightSnapshot?.horses ?? []),
    [horseWeightSnapshot],
  );
  const filteredResults = useMemo(
    () => filterHorseRaceResultsToRecentYears(results, CHART_WINDOW_YEARS),
    [results],
  );
  const seriesListsByMetric = useMemo(
    () =>
      buildSeriesListsByMetric({
        results: filteredResults,
        runners,
        targetKeibajoCode,
        targetRaceDate,
        upcomingWeights,
      }),
    [filteredResults, runners, targetKeibajoCode, targetRaceDate, upcomingWeights],
  );
  const chipSeriesList = seriesListsByMetric.finish;
  const selectedKetto = selectedHorse ?? chipSeriesList.at(0)?.kettoTorokuBango ?? "";
  const correlationRows = useMemo(
    () =>
      buildHorseRaceCorrelationRows({
        kettoTorokuBango: selectedKetto,
        results: filteredResults,
        runners,
        targetKeibajoCode,
        targetRaceDate,
        upcomingWeights,
      }),
    [filteredResults, runners, selectedKetto, targetKeibajoCode, targetRaceDate, upcomingWeights],
  );
  if (filteredResults.length === 0) {
    return <p className="empty-state">表示できる競走成績がありません</p>;
  }
  const isOverview = viewMode === "overview";
  const toggleHorse = (kettoTorokuBango: string) => {
    setHiddenHorses((current) => {
      const next = new Set(current);
      if (next.has(kettoTorokuBango)) {
        next.delete(kettoTorokuBango);
        return next;
      }
      next.add(kettoTorokuBango);
      return next;
    });
  };
  const isChipPressed = (kettoTorokuBango: string): boolean =>
    isOverview ? !hiddenHorses.has(kettoTorokuBango) : selectedKetto === kettoTorokuBango;
  const handleChipClick = (kettoTorokuBango: string) => {
    if (isOverview) {
      toggleHorse(kettoTorokuBango);
      return;
    }
    setSelectedHorse(kettoTorokuBango);
  };
  return (
    <div className="race-results-chart">
      <div className="race-results-chart-controls">
        <div
          aria-label="表示形式の切替"
          className="stats-section-toggle-wrap race-results-chart-view-toggle"
          role="group"
        >
          {CHART_VIEW_MODES.map((mode) => (
            <button
              aria-pressed={viewMode === mode}
              className="stats-control-button"
              key={mode}
              type="button"
              onClick={() => {
                setViewMode(mode);
              }}
            >
              {CHART_VIEW_MODE_LABELS[mode]}
            </button>
          ))}
        </div>
        {isOverview && (
          <div aria-label="全馬の表示切替" className="stats-section-toggle-wrap" role="group">
            <button
              className="stats-control-button"
              type="button"
              onClick={() => {
                setHiddenHorses(new Set<string>());
              }}
            >
              全馬表示
            </button>
            <button
              className="stats-control-button"
              type="button"
              onClick={() => {
                setHiddenHorses(new Set(chipSeriesList.map((series) => series.kettoTorokuBango)));
              }}
            >
              全馬非表示
            </button>
          </div>
        )}
        <div
          aria-label={CHIP_GROUP_LABELS[viewMode]}
          className="stats-section-toggle-wrap"
          role="group"
        >
          {chipSeriesList.map((series) => (
            <button
              aria-pressed={isChipPressed(series.kettoTorokuBango)}
              className="stats-control-button race-results-chart-chip"
              key={series.kettoTorokuBango}
              type="button"
              onClick={() => {
                handleChipClick(series.kettoTorokuBango);
              }}
            >
              <span
                aria-hidden="true"
                className="race-results-chart-swatch"
                style={{ backgroundColor: resolveSeriesStroke(series) }}
              />
              {getHorseChipLabel(series)}
            </button>
          ))}
        </div>
      </div>
      {isOverview ? (
        <OverviewPanels hiddenHorses={hiddenHorses} seriesListsByMetric={seriesListsByMetric} />
      ) : (
        <HorseRaceResultsCorrelationMatrix rows={correlationRows} />
      )}
    </div>
  );
};
