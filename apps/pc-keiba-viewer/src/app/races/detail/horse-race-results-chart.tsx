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
  HorseRaceChartSeries,
} from "../../../lib/horse-race-results-chart-data";
import type { HorseRaceResult } from "../../../lib/race-types";
import { HorseRaceResultsCorrelationMatrix } from "./horse-race-results-correlation-matrix";

type ChartViewMode = "overview" | "correlation";

interface HorseRaceResultsChartProps {
  results: HorseRaceResult[];
}

interface ChartInitialDimension {
  height: number;
  width: number;
}

interface ChartLineDot {
  r: number;
}

interface OverviewPanelsProps {
  hiddenHorses: ReadonlySet<string>;
  seriesListsByMetric: Record<HorseRaceChartMetric, HorseRaceChartSeries[]>;
}

type ChartYAxisDomain = [number, "auto"] | ["auto", "auto"];

const CHART_WINDOW_YEARS = 3;
const CHART_PANEL_HEIGHT = 260;
const CHART_GRID_STROKE = "#d8e0da";
const CHART_GRID_DASH = "3 3";
const CHART_LINE_STROKE_WIDTH = 1.5;
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

const getHorseChipLabel = (series: HorseRaceChartSeries): string =>
  `${series.umaban ?? "-"} ${series.bamei}`;

// Horse grouping is metric-independent, so every list shares the same horses
// in the same order; only the plotted points differ between metrics.
const buildSeriesListsByMetric = (
  recentResults: HorseRaceResult[],
): Record<HorseRaceChartMetric, HorseRaceChartSeries[]> => ({
  finish: buildHorseRaceChartSeriesList(recentResults, "finish"),
  popularity: buildHorseRaceChartSeriesList(recentResults, "popularity"),
  weight: buildHorseRaceChartSeriesList(recentResults, "weight"),
  weightDelta: buildHorseRaceChartSeriesList(recentResults, "weightDelta"),
});

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
            <Tooltip
              formatter={(value) => `${String(value)}${HORSE_RACE_CHART_METRIC_UNITS[metric]}`}
              labelFormatter={(label) => formatHorseRaceChartDate(Number(label))}
            />
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
                  stroke={series.color}
                  strokeWidth={CHART_LINE_STROKE_WIDTH}
                  type="monotone"
                />
              ))}
          </LineChart>
        </ResponsiveContainer>
      </section>
    ))}
  </div>
);

export const HorseRaceResultsChart = ({ results }: HorseRaceResultsChartProps) => {
  const [viewMode, setViewMode] = useState<ChartViewMode>("overview");
  const [hiddenHorses, setHiddenHorses] = useState<ReadonlySet<string>>(new Set<string>());
  const [selectedHorse, setSelectedHorse] = useState<string | null>(null);
  const filteredResults = useMemo(
    () => filterHorseRaceResultsToRecentYears(results, CHART_WINDOW_YEARS),
    [results],
  );
  const seriesListsByMetric = useMemo(
    () => buildSeriesListsByMetric(filteredResults),
    [filteredResults],
  );
  const chipSeriesList = seriesListsByMetric.finish;
  const selectedKetto = selectedHorse ?? chipSeriesList.at(0)?.kettoTorokuBango ?? "";
  const correlationRows = useMemo(
    () => buildHorseRaceCorrelationRows(filteredResults, selectedKetto),
    [filteredResults, selectedKetto],
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
                style={{ backgroundColor: series.color }}
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
