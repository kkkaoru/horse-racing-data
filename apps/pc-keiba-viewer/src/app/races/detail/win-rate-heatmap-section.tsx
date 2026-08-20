"use client";

import { memo, useState } from "react";

import type {
  BloodlineStatsRow,
  FrameStatsRow,
  HorseRaceResult,
  Runner,
  SimilarRaceStatsRow,
} from "../../../lib/race-types";
import {
  buildWinRateHeatmapRows,
  DEFAULT_WIN_RATE_HEATMAP_VIEW_MODE,
  formatWinRateHeatmapValue,
  getVisibleWinRateHeatmapRateMetrics,
  getWinRateHeatmapTooltipName,
  WIN_RATE_HEATMAP_COLUMNS,
  WIN_RATE_HEATMAP_VIEW_MODES,
  winRateHeatmapBackground,
  winRateHeatmapEntityColSpan,
  type WinRateHeatmapCell,
  type WinRateHeatmapColumn,
  type WinRateHeatmapRateMetric,
  type WinRateHeatmapViewMode,
} from "../../../lib/win-rate-heatmap";
import { FrameNumberBadge } from "./frame-number-badge";

interface WinRateHeatmapSectionProps {
  bloodlineRows: BloodlineStatsRow[];
  frameStats: FrameStatsRow[];
  horseResults: HorseRaceResult[];
  runners: Runner[];
  similarRows: SimilarRaceStatsRow[];
}

const WIN_RATE_HEATMAP_VIEW_RADIO_NAME = "win-rate-heatmap-view";

interface WinRateHeatmapSwatchProps {
  cell: WinRateHeatmapCell;
  column: WinRateHeatmapColumn;
  frameNumber: string;
  isOpen: boolean;
  metric: WinRateHeatmapRateMetric;
  onToggle: () => void;
}

const WinRateHeatmapSwatch = ({
  cell,
  column,
  frameNumber,
  isOpen,
  metric,
  onToggle,
}: WinRateHeatmapSwatchProps) => {
  const value = cell[metric.key];
  return (
    <td
      className={isOpen ? "win-rate-heatmap-swatch tooltip-open" : "win-rate-heatmap-swatch"}
      style={{ backgroundColor: winRateHeatmapBackground(value, metric.hue) }}
    >
      <button
        aria-label={
          column.key === "frame"
            ? `${column.label} ${frameNumber}`
            : `${column.label} ${getWinRateHeatmapTooltipName(cell)}`
        }
        className="win-rate-heatmap-swatch-button"
        type="button"
        onClick={onToggle}
      >
        <span className="win-rate-heatmap-swatch-value">{formatWinRateHeatmapValue(value)}</span>
        <span className="win-rate-heatmap-tooltip" role="tooltip">
          {column.key === "frame" ? (
            <FrameNumberBadge value={frameNumber} />
          ) : (
            getWinRateHeatmapTooltipName(cell)
          )}
        </span>
      </button>
    </td>
  );
};

export const WinRateHeatmapSection = memo(function WinRateHeatmapSection({
  bloodlineRows,
  frameStats,
  horseResults,
  runners,
  similarRows,
}: WinRateHeatmapSectionProps) {
  const [openTooltipKey, setOpenTooltipKey] = useState<string | null>(null);
  const [viewMode, setViewMode] = useState<WinRateHeatmapViewMode>(
    DEFAULT_WIN_RATE_HEATMAP_VIEW_MODE,
  );
  const visibleRateMetrics = getVisibleWinRateHeatmapRateMetrics(viewMode);
  const entityColSpan = winRateHeatmapEntityColSpan(visibleRateMetrics.length);
  const rows = buildWinRateHeatmapRows({
    bloodlineRows,
    frameStats,
    horseResults,
    runners,
    similarRows,
  });
  if (rows.length === 0) {
    return <p className="empty-state">勝率ヒートマップを表示する出走馬がありません。</p>;
  }
  return (
    <div className="win-rate-heatmap-wrap">
      <fieldset aria-label="表示指標" className="win-rate-heatmap-view-toggle">
        {WIN_RATE_HEATMAP_VIEW_MODES.map((mode) => (
          <label className="running-style-bucket-toggle-label" key={mode.key}>
            <input
              checked={viewMode === mode.key}
              name={WIN_RATE_HEATMAP_VIEW_RADIO_NAME}
              type="radio"
              value={mode.key}
              onChange={() => {
                setViewMode(mode.key);
              }}
            />
            {mode.label}
          </label>
        ))}
      </fieldset>
      <div className="stats-table-wrap win-rate-heatmap-table-wrap">
        <table className="stats-table win-rate-heatmap-table">
          <colgroup>
            <col className="win-rate-heatmap-col-number" />
          </colgroup>
          <thead>
            <tr>
              <th className="win-rate-heatmap-number" rowSpan={2} scope="col">
                番
              </th>
              {WIN_RATE_HEATMAP_COLUMNS.map((column) => (
                <th key={column.key} colSpan={entityColSpan} scope="colgroup">
                  {column.label}
                </th>
              ))}
            </tr>
            <tr>
              {WIN_RATE_HEATMAP_COLUMNS.map((column) =>
                visibleRateMetrics.map((metric) => (
                  <th
                    className="win-rate-heatmap-rate-heading"
                    key={`${column.key}-${metric.key}`}
                    scope="col"
                  >
                    {metric.shortLabel}
                  </th>
                )),
              )}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.horseNumber}>
                <th className="win-rate-heatmap-number" scope="row" title={row.horseName}>
                  {row.horseNumber}
                </th>
                {WIN_RATE_HEATMAP_COLUMNS.map((column) => {
                  const cell = row.cells[column.key];
                  return visibleRateMetrics.map((metric) => {
                    const tooltipKey = `${row.horseNumber}-${column.key}-${metric.key}`;
                    return (
                      <WinRateHeatmapSwatch
                        cell={cell}
                        column={column}
                        frameNumber={row.frameNumber}
                        isOpen={openTooltipKey === tooltipKey}
                        key={tooltipKey}
                        metric={metric}
                        onToggle={() => {
                          setOpenTooltipKey((current) =>
                            current === tooltipKey ? null : tooltipKey,
                          );
                        }}
                      />
                    );
                  });
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
});
