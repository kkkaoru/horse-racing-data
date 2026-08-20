"use client";

import { memo, useMemo, useState } from "react";

import { indexLiveHorseWeightKg } from "../../../lib/horse-weight-class";
import { useHorseWeightStream } from "../../../lib/horse-weight-stream-client";
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
  getVisibleWinRateHeatmapColumns,
  getVisibleWinRateHeatmapRateMetrics,
  getWinRateHeatmapTooltipName,
  shouldShowWinRateHeatmapWeightColumn,
  WIN_RATE_HEATMAP_VIEW_MODES,
  winRateHeatmapBackground,
  winRateHeatmapEntityColSpan,
  type WinRateHeatmapCell,
  type WinRateHeatmapColumn,
  type WinRateHeatmapRateMetric,
  type WinRateHeatmapViewMode,
} from "../../../lib/win-rate-heatmap";
import { FrameNumberBadge } from "./frame-number-badge";
import { useRealtimeRacePayload, type RealtimeRaceRequest } from "./realtime-client";

interface WinRateHeatmapSectionProps {
  bloodlineRows: BloodlineStatsRow[];
  frameStats: FrameStatsRow[];
  horseResults: HorseRaceResult[];
  keibajoCode: string;
  realtimeRequest: RealtimeRaceRequest;
  runners: Runner[];
  similarRows: SimilarRaceStatsRow[];
}

const WIN_RATE_HEATMAP_VIEW_RADIO_NAME = "win-rate-heatmap-view";

interface WinRateHeatmapSwatchProps {
  cell: WinRateHeatmapCell;
  column: WinRateHeatmapColumn;
  frameNumber: string;
  isLastRow: boolean;
  isOpen: boolean;
  metric: WinRateHeatmapRateMetric;
  onToggle: () => void;
}

const heatmapSwatchClassName = (input: { isLastRow: boolean; isOpen: boolean }): string => {
  if (input.isLastRow && input.isOpen) {
    return "win-rate-heatmap-swatch win-rate-heatmap-tooltip-above tooltip-open";
  }
  if (input.isLastRow) {
    return "win-rate-heatmap-swatch win-rate-heatmap-tooltip-above";
  }
  if (input.isOpen) {
    return "win-rate-heatmap-swatch tooltip-open";
  }
  return "win-rate-heatmap-swatch";
};

const WinRateHeatmapSwatch = ({
  cell,
  column,
  frameNumber,
  isLastRow,
  isOpen,
  metric,
  onToggle,
}: WinRateHeatmapSwatchProps) => {
  const value = cell[metric.key];
  return (
    <td
      className={heatmapSwatchClassName({ isLastRow, isOpen })}
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
  keibajoCode,
  realtimeRequest,
  runners,
  similarRows,
}: WinRateHeatmapSectionProps) {
  const [openTooltipKey, setOpenTooltipKey] = useState<string | null>(null);
  const [viewMode, setViewMode] = useState<WinRateHeatmapViewMode>(
    DEFAULT_WIN_RATE_HEATMAP_VIEW_MODE,
  );
  const { payload: realtimePayload } = useRealtimeRacePayload(realtimeRequest, null);
  const horseWeightSnapshot = useHorseWeightStream({
    day: realtimeRequest.day,
    initial: realtimePayload?.horseWeights ?? null,
    keibajoCode: realtimeRequest.keibajoCode,
    month: realtimeRequest.month,
    raceNumber: realtimeRequest.raceNumber,
    source: realtimeRequest.source,
    year: realtimeRequest.year,
  });
  const liveWeightKgByHorse = useMemo(
    () =>
      indexLiveHorseWeightKg(
        horseWeightSnapshot?.horses ?? realtimePayload?.horseWeights?.horses ?? [],
      ),
    [horseWeightSnapshot, realtimePayload],
  );
  const visibleRateMetrics = getVisibleWinRateHeatmapRateMetrics(viewMode);
  const entityColSpan = winRateHeatmapEntityColSpan(visibleRateMetrics.length);
  const showWeight = shouldShowWinRateHeatmapWeightColumn({
    keibajoCode,
    liveWeightKgByHorse,
    runners,
  });
  const visibleColumns = getVisibleWinRateHeatmapColumns(showWeight);
  const rows = buildWinRateHeatmapRows({
    bloodlineRows,
    frameStats,
    horseResults,
    keibajoCode,
    liveWeightKgByHorse,
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
              {visibleColumns.map((column) => (
                <th key={column.key} colSpan={entityColSpan} scope="colgroup">
                  {column.label}
                </th>
              ))}
            </tr>
            <tr>
              {visibleColumns.map((column) =>
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
            {rows.map((row, rowIndex) => (
              <tr key={row.horseNumber}>
                <th className="win-rate-heatmap-number" scope="row" title={row.horseName}>
                  {row.horseNumber}
                </th>
                {visibleColumns.map((column) => {
                  const cell = row.cells[column.key];
                  return visibleRateMetrics.map((metric) => {
                    const tooltipKey = `${row.horseNumber}-${column.key}-${metric.key}`;
                    return (
                      <WinRateHeatmapSwatch
                        cell={cell}
                        column={column}
                        frameNumber={row.frameNumber}
                        isLastRow={rowIndex === rows.length - 1}
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
