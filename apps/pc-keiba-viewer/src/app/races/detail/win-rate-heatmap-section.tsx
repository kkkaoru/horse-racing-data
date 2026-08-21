"use client";

import { memo, useEffect, useMemo, useState, useSyncExternalStore } from "react";

import { indexLiveHorseWeightKg } from "../../../lib/horse-weight-class";
import { useHorseWeightStream } from "../../../lib/horse-weight-stream-client";
import type {
  BloodlineStatsRow,
  FrameStatsRow,
  HorseRaceResult,
  Runner,
  SimilarRaceStatsRow,
  WeightClassStatsRow,
} from "../../../lib/race-types";
import {
  buildWinRateHeatmapColorScaleGradient,
  buildWinRateHeatmapRows,
  DEFAULT_WIN_RATE_HEATMAP_VIEW_MODE,
  EMPTY_WIN_RATE_HEATMAP_CELL,
  formatWinRateHeatmapColorScaleAriaLabel,
  formatWinRateHeatmapColorScaleCaption,
  formatWinRateHeatmapColorScaleTick,
  formatWinRateHeatmapTooltipStarts,
  formatWinRateHeatmapValue,
  getVisibleWinRateHeatmapColumns,
  getVisibleWinRateHeatmapRateMetrics,
  getWinRateHeatmapColorScaleTracks,
  getWinRateHeatmapTooltipName,
  shouldShowWinRateHeatmapCarriedWeightColumn,
  shouldShowWinRateHeatmapWeightColumn,
  WIN_RATE_HEATMAP_COLOR_SCALE_TICKS,
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
  carriedWeightClassStats?: readonly WeightClassStatsRow[];
  frameStats: FrameStatsRow[];
  horseResults: HorseRaceResult[];
  keibajoCode: string;
  realtimeRequest: RealtimeRaceRequest;
  runners: Runner[];
  similarRows: SimilarRaceStatsRow[];
  weightClassStats?: readonly WeightClassStatsRow[];
}

interface WinRateHeatmapSwatchProps {
  cell: WinRateHeatmapCell;
  column: WinRateHeatmapColumn;
  compactValue: boolean;
  frameNumber: string;
  isLastRow: boolean;
  isOpen: boolean;
  metric: WinRateHeatmapRateMetric;
  onToggle: () => void;
  showStarts: boolean;
}

interface WinRateHeatmapColorScaleProps {
  metrics: readonly WinRateHeatmapRateMetric[];
}

const WIN_RATE_HEATMAP_VIEW_RADIO_NAME = "win-rate-heatmap-view";
const HEATMAP_MOBILE_TOOLTIP_QUERY = "(max-width: 720px)";
const DEFAULT_WIN_RATE_HEATMAP_SHOW_STARTS: boolean = false;

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

const subscribeHeatmapMobileTooltip = (onStoreChange: () => void): (() => void) => {
  if (typeof window === "undefined" || !window.matchMedia) {
    return () => {};
  }
  const mediaQuery = window.matchMedia(HEATMAP_MOBILE_TOOLTIP_QUERY);
  if (mediaQuery.addEventListener) {
    mediaQuery.addEventListener("change", onStoreChange);
  } else {
    mediaQuery.addListener(onStoreChange);
  }
  return () => {
    if (mediaQuery.removeEventListener) {
      mediaQuery.removeEventListener("change", onStoreChange);
    } else {
      mediaQuery.removeListener(onStoreChange);
    }
  };
};

const getHeatmapMobileTooltipSnapshot = (): boolean =>
  typeof window !== "undefined" &&
  Boolean(window.matchMedia) &&
  window.matchMedia(HEATMAP_MOBILE_TOOLTIP_QUERY).matches;

const getHeatmapMobileTooltipServerSnapshot = (): boolean => false;

const WinRateHeatmapColorScale = ({ metrics }: WinRateHeatmapColorScaleProps) => (
  <div className="win-rate-heatmap-color-scale-slot">
    <figure
      aria-label={formatWinRateHeatmapColorScaleAriaLabel(metrics)}
      className="win-rate-heatmap-color-scale"
    >
      <figcaption className="win-rate-heatmap-color-scale-heading">
        {formatWinRateHeatmapColorScaleCaption(metrics)}
      </figcaption>
      <div className="win-rate-heatmap-color-scale-tracks">
        {getWinRateHeatmapColorScaleTracks(metrics).map((metric) => (
          <div
            className="win-rate-heatmap-color-scale-track win-rate-heatmap-color-scale-track-solo"
            key={metric.key}
          >
            <div
              className="win-rate-heatmap-color-scale-bar"
              style={{ backgroundImage: buildWinRateHeatmapColorScaleGradient(metric.hue) }}
            />
          </div>
        ))}
      </div>
      <div className="win-rate-heatmap-color-scale-ticks">
        {WIN_RATE_HEATMAP_COLOR_SCALE_TICKS.map((rate) => (
          <span key={rate}>{formatWinRateHeatmapColorScaleTick(rate)}</span>
        ))}
      </div>
    </figure>
  </div>
);

const heatmapSwatchAriaLabel = (input: {
  cell: WinRateHeatmapCell;
  column: WinRateHeatmapColumn;
  frameNumber: string;
  startsLabel: string | null;
}): string => {
  const nameLabel =
    input.column.key === "frame"
      ? `${input.column.label} ${input.frameNumber}`
      : `${input.column.label} ${getWinRateHeatmapTooltipName(input.cell)}`;
  return input.startsLabel === null ? nameLabel : `${nameLabel} ${input.startsLabel}`;
};

const WinRateHeatmapSwatch = ({
  cell,
  column,
  compactValue,
  frameNumber,
  isLastRow,
  isOpen,
  metric,
  onToggle,
  showStarts,
}: WinRateHeatmapSwatchProps) => {
  const value = cell[metric.key];
  const startsLabel = showStarts ? formatWinRateHeatmapTooltipStarts(cell.starts) : null;
  return (
    <td
      className={heatmapSwatchClassName({ isLastRow, isOpen })}
      style={{ backgroundColor: winRateHeatmapBackground(value, metric.hue) }}
    >
      <button
        aria-label={heatmapSwatchAriaLabel({ cell, column, frameNumber, startsLabel })}
        aria-expanded={isOpen}
        className="win-rate-heatmap-swatch-button"
        type="button"
        onClick={onToggle}
      >
        <span className="win-rate-heatmap-swatch-value">
          {formatWinRateHeatmapValue(value, compactValue)}
        </span>
        <span className="win-rate-heatmap-tooltip" role="tooltip">
          {column.key === "frame" ? (
            <FrameNumberBadge value={frameNumber} />
          ) : (
            getWinRateHeatmapTooltipName(cell)
          )}
          {startsLabel === null ? null : (
            <span className="win-rate-heatmap-tooltip-starts">{startsLabel}</span>
          )}
        </span>
      </button>
    </td>
  );
};

export const WinRateHeatmapSection = memo(function WinRateHeatmapSection({
  bloodlineRows,
  carriedWeightClassStats,
  frameStats,
  horseResults,
  keibajoCode,
  realtimeRequest,
  runners,
  similarRows,
  weightClassStats,
}: WinRateHeatmapSectionProps) {
  const [openTooltipKey, setOpenTooltipKey] = useState<string | null>(null);
  const [showStarts, setShowStarts] = useState(DEFAULT_WIN_RATE_HEATMAP_SHOW_STARTS);
  const [viewMode, setViewMode] = useState<WinRateHeatmapViewMode>(
    DEFAULT_WIN_RATE_HEATMAP_VIEW_MODE,
  );
  const isMobileTooltip = useSyncExternalStore(
    subscribeHeatmapMobileTooltip,
    getHeatmapMobileTooltipSnapshot,
    getHeatmapMobileTooltipServerSnapshot,
  );
  useEffect(() => {
    if (isMobileTooltip) {
      return undefined;
    }
    setOpenTooltipKey(null);
    return undefined;
  }, [isMobileTooltip]);
  useEffect(() => {
    if (!isMobileTooltip || openTooltipKey === null) {
      return undefined;
    }
    const closeTooltipOnOutsidePointer = (event: PointerEvent): void => {
      const target = event.target;
      if (target instanceof Element && target.closest(".win-rate-heatmap-swatch-button") !== null) {
        return;
      }
      setOpenTooltipKey(null);
    };
    document.addEventListener("pointerdown", closeTooltipOnOutsidePointer);
    return () => {
      document.removeEventListener("pointerdown", closeTooltipOnOutsidePointer);
    };
  }, [isMobileTooltip, openTooltipKey]);
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
  const showCarriedWeight = shouldShowWinRateHeatmapCarriedWeightColumn({
    keibajoCode,
    runners,
  });
  const visibleColumns = getVisibleWinRateHeatmapColumns({
    showCarriedWeight,
    showWeight,
  });
  const rows = buildWinRateHeatmapRows({
    bloodlineRows,
    carriedWeightClassStats,
    frameStats,
    horseResults,
    keibajoCode,
    liveWeightKgByHorse,
    runners,
    similarRows,
    weightClassStats,
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
        <label className="running-style-bucket-toggle-label">
          <input
            checked={showStarts}
            type="checkbox"
            onChange={() => {
              setShowStarts((current) => !current);
            }}
          />
          レース数
        </label>
      </fieldset>
      <WinRateHeatmapColorScale metrics={visibleRateMetrics} />
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
                  const cell = row.cells[column.key] ?? EMPTY_WIN_RATE_HEATMAP_CELL;
                  return visibleRateMetrics.map((metric) => {
                    const tooltipKey = `${row.horseNumber}-${column.key}-${metric.key}`;
                    return (
                      <WinRateHeatmapSwatch
                        cell={cell}
                        column={column}
                        compactValue={viewMode === "all"}
                        frameNumber={row.frameNumber}
                        isLastRow={rowIndex === rows.length - 1}
                        isOpen={isMobileTooltip && openTooltipKey === tooltipKey}
                        key={tooltipKey}
                        metric={metric}
                        showStarts={showStarts}
                        onToggle={() => {
                          if (!isMobileTooltip) {
                            return;
                          }
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
