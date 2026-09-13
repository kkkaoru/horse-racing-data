// Run with bun (bunx vitest).
import {
  buildWinRateHeatmapDisplay,
  buildWinRateHeatmapRows,
  type BuildWinRateHeatmapRowsInput,
  type WinRateHeatmapDisplayModel,
  type WinRateHeatmapRow,
  type WinRateHeatmapViewMode,
} from "./win-rate-heatmap";

export interface HeatmapPresentation {
  displays: Record<string, WinRateHeatmapDisplayModel>;
  rows: WinRateHeatmapRow[];
  combinedBloodlineRows: WinRateHeatmapRow[];
  version: 1;
}

export interface HeatmapDisplaySelection {
  showStarts: boolean;
  splitBloodlineLines: boolean;
  viewMode: WinRateHeatmapViewMode;
}

const VIEW_MODES: readonly WinRateHeatmapViewMode[] = [
  "winRate",
  "quinellaRate",
  "showRate",
  "all",
];
const BOOLEAN_OPTIONS: readonly boolean[] = [false, true];

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

// Presentation artifacts are produced by our warm worker, not accepted from clients.
// Reject incomplete envelopes rather than triggering a request-time rebuild.
export const isHeatmapPresentation = (value: unknown): value is HeatmapPresentation =>
  isRecord(value) &&
  value.version === 1 &&
  Array.isArray(value.rows) &&
  Array.isArray(value.combinedBloodlineRows) &&
  isRecord(value.displays) &&
  Object.values(value.displays).every(
    (display) =>
      isRecord(display) &&
      Array.isArray(display.rows) &&
      Array.isArray(display.visibleColumns) &&
      Array.isArray(display.visibleRateMetrics) &&
      isRecord(display.colorScales) &&
      typeof display.empty === "boolean" &&
      typeof display.entityColSpan === "number" &&
      typeof display.showWeight === "boolean" &&
      typeof display.showCarriedWeight === "boolean" &&
      VIEW_MODES.some((mode) => display.viewMode === mode),
  );

export const heatmapDisplayKey = (selection: HeatmapDisplaySelection): string =>
  `${selection.viewMode}:${Number(selection.showStarts)}:${Number(selection.splitBloodlineLines)}`;

// This producer is used only during warming. Readers select a stored variant;
// neither percentages nor display labels/colors need to be calculated again.
export const buildHeatmapPresentation = (
  input: BuildWinRateHeatmapRowsInput,
): HeatmapPresentation => ({
  combinedBloodlineRows: buildWinRateHeatmapRows({ ...input, splitBloodlineLines: false }),
  displays: Object.fromEntries(
    VIEW_MODES.flatMap((viewMode) =>
      BOOLEAN_OPTIONS.flatMap((showStarts) =>
        BOOLEAN_OPTIONS.map((splitBloodlineLines) => [
          heatmapDisplayKey({ showStarts, splitBloodlineLines, viewMode }),
          buildWinRateHeatmapDisplay({ ...input, showStarts, splitBloodlineLines, viewMode }),
        ]),
      ),
    ),
  ),
  rows: buildWinRateHeatmapRows({ ...input, splitBloodlineLines: true }),
  version: 1,
});

export const selectHeatmapDisplay = (
  presentation: HeatmapPresentation,
  selection: HeatmapDisplaySelection,
): WinRateHeatmapDisplayModel | null => presentation.displays[heatmapDisplayKey(selection)] ?? null;
