"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";

import type { RaceSource } from "../../../lib/codes";
import { fetchWithRetry } from "../../../lib/fetch-with-retry";
import { formatKeibajo, formatRaceNumber } from "../../../lib/format";
import { getRaceTrendLiveUrl } from "../../../lib/paddock-client-url";
import {
  aggregateForTargets,
  normalizeNumberText,
  parseStoredPopularity,
  parseStoredWinOdds,
  resolveRowJockeyKey,
  runningStyleFromCorners,
  starterRunningStyleKey,
} from "../../../lib/race-trend-aggregate";
import { RACE_TREND_CACHE_REFRESH_PARAM } from "../../../lib/race-trend-cache";
import {
  clearRaceTrendScoreConditionsQueryParam,
  clearRaceTrendSortKeyQueryParam,
  clearRaceTrendTargetQueryParams,
  DEFAULT_RACE_TREND_SCORE_CONDITIONS_QUERY,
  DEFAULT_RACE_TREND_SORT_KEY,
  DEFAULT_RACE_TREND_TARGETS,
  getRaceTrendScoreConditionsFromSearchParams,
  getRaceTrendSortKeyFromSearchParams,
  getRaceTrendTargetsFromSearchParams,
  isDefaultRaceTrendScoreConditionsQuery,
  isDefaultRaceTrendSortKey,
  isDefaultRaceTrendTargets,
  isSameRaceTrendScoreConditionsQuery,
  isSameRaceTrendTargets,
  RACE_TREND_SCORE_CONDITION_QUERY_KEYS,
  RACE_TREND_SCORE_CONDITIONS_QUERY_PARAM,
  RACE_TREND_SORT_KEYS,
  RACE_TREND_SORT_QUERY_PARAM,
  RACE_TREND_TARGET_KEYS,
  RACE_TREND_TARGET_QUERY_PARAM,
  serializeRaceTrendScoreConditionsQuery,
  serializeRaceTrendSortKeyQuery,
  serializeRaceTrendTargets,
  type RaceTrendScoreConditionKey,
  type RaceTrendScoreConditionsQuery,
  type RaceTrendSortKey,
  type RaceTrendTargetKey,
  type RaceTrendTargets,
} from "../../../lib/race-trend-query";
import {
  computeRawUmabanScores,
  normalizeUmabanScores,
  type ScoreDetailInput,
  type UmabanContext,
} from "../../../lib/race-trend-score";
import type {
  RaceTrendCurrentRunningStyle,
  RaceTrendDetail,
  RaceTrendRawPayload,
  RaceTrendRunnerSummary,
  RaceTrendRunningStyle,
  RaceTrendRunningStyleCache,
  RaceTrendRunningStyleRow,
  RaceTrendStarterRow,
} from "../../../lib/race-types";
import { useRealtimeRaceSelector } from "./realtime-client";

const RACE_TREND_RETRY_OPTIONS = {
  baseDelayMs: 300,
  maxAttempts: 4,
  maxDelayMs: 4000,
} as const;

const RACE_TREND_AUTO_REFRESH_INTERVAL_MS = 60_000;

interface RaceTrendSectionProps {
  day: string;
  defaultEndDate: string;
  defaultStartDate: string;
  initialScoreConditions?: RaceTrendScoreConditionsQuery;
  initialTrendTargets?: RaceTrendTargets;
  keibajoCode: string;
  minStartDate: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  year: string;
}

const clampIsoDateToRange = (value: string, min: string, max: string): string => {
  if (value < min) return min;
  if (value > max) return max;
  return value;
};

const TREND_TARGET_LABELS: Record<RaceTrendTargetKey, string> = {
  runningStyle: "脚質",
  frame: "枠",
  jockey: "騎手",
  raceNumber: "レース番号",
};

const SCORE_CONDITION_LABELS: Record<RaceTrendScoreConditionKey, string> = {
  frame: "枠",
  jockey: "騎手",
  frameRunningStyle: "枠+脚質",
};

const SCORE_PLACEHOLDER = "-";
const SCORE_DECIMAL_PLACES = 2;
const RACE_TREND_SCORE_TOOLTIP_ID = "race-trend-score-tooltip";
const RACE_TREND_SCORE_TOOLTIP_TEXT =
  "過去レースの「人気 − 着順」を元に算出する相対スコア。 3 着以内はオッズ重み付きで大きくプラス、 4-5 着は人気とオッズの動的計算で中程度プラス、 6 着以下は無評価。 「スコア条件」 (枠 / 騎手 / 枠+脚質) で過去レースを絞り込み、 選択条件ごとのスコアを平均して、 全馬番で 0.00 〜 1.00 の相対値に正規化します。";
const RACE_TREND_TOOLTIP_OFFSET_PX = 8;

const SORT_KEY_LABELS: Record<RaceTrendSortKey, string> = {
  score: "スコア",
  showRate: "複勝率",
  quinellaRate: "連対率",
  winRate: "勝率",
};

const SCORE_DETAIL_HEADING = "スコア対象レコード";
const SCORE_DETAIL_EMPTY_MESSAGE = "スコア条件が選択されていません";

const RUNNING_STYLE_LABELS: Record<RaceTrendRunningStyle, string> = {
  nige: "逃げ",
  senkou: "先行",
  sashi: "差し",
  oikomi: "追込",
};

const formatRate = (value: number): string => `${value.toFixed(1)}%`;

const formatMedian = (value: number | null | undefined): string => {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "-";
  }
  return Number.isInteger(value) ? String(value) : value.toFixed(1);
};

const formatTrendWinOdds = (value: number | null | undefined): string =>
  typeof value === "number" && Number.isFinite(value) ? value.toFixed(1) : "-";

const formatRunningStyle = (value: RaceTrendRunningStyle | null): string =>
  value ? RUNNING_STYLE_LABELS[value] : "-";

const normalizeHorseNumber = (value: string | null | undefined): string => {
  const parsed = Number(value ?? "");
  return Number.isFinite(parsed) && parsed > 0 ? String(parsed) : "";
};

const formatHorseWeight = (weight: number | null, delta: number | null): string => {
  if (weight === null) return "-";
  if (delta === null) return String(weight);
  const sign = delta > 0 ? "+" : delta < 0 ? "" : "±";
  return `${weight}(${sign}${delta})`;
};

const isRaceTrendRawPayload = (value: unknown): value is RaceTrendRawPayload =>
  typeof value === "object" &&
  value !== null &&
  "starterRows" in value &&
  Array.isArray((value as { starterRows?: unknown }).starterRows);

const buildCurrentRunningStyleMap = (
  rows: RaceTrendCurrentRunningStyle[],
): Map<string, RaceTrendRunningStyle> =>
  new Map(rows.map((row) => [row.horseNumber, row.predictedLabel]));

const buildHistoricalRunningStyleMap = (
  rows: RaceTrendRunningStyleCache[],
): Map<string, RaceTrendRunningStyle> =>
  new Map(
    rows.map((row) => [
      `${row.raceKey}:${normalizeNumberText(row.horseNumber) ?? ""}`,
      row.predictedLabel,
    ]),
  );

const buildScoreContext = (
  runner: RaceTrendRunnerSummary,
  currentRunningStyleMap: Map<string, RaceTrendRunningStyle>,
): UmabanContext => {
  const umaban = normalizeNumberText(runner.horseNumber) ?? "";
  return {
    umaban,
    frameNumber: normalizeNumberText(runner.frameNumber),
    jockeyKey: resolveRowJockeyKey(runner.jockeyName),
    runningStyle: currentRunningStyleMap.get(umaban) ?? null,
  };
};

interface BuildScoreDetailParams {
  row: RaceTrendStarterRow;
  runningStyleByStarterKey: Map<string, RaceTrendRunningStyle>;
}

const buildScoreDetail = (params: BuildScoreDetailParams): ScoreDetailInput => {
  const { row, runningStyleByStarterKey } = params;
  const runningStyleFromMap = runningStyleByStarterKey.get(starterRunningStyleKey(row));
  return {
    popularity: parseStoredPopularity(row.tanshoPopularity),
    finishPosition: row.finishPosition,
    winOdds: parseStoredWinOdds(row.tanshoOdds),
    frameNumber: normalizeNumberText(row.wakuban),
    jockeyKey: resolveRowJockeyKey(row.jockeyName),
    runningStyle: runningStyleFromMap ?? runningStyleFromCorners(row),
  };
};

const hasValidUmaban = (context: UmabanContext): boolean => context.umaban !== "";
const hasFinishPosition = (detail: ScoreDetailInput): boolean => detail.finishPosition > 0;

interface FormatScoreParams {
  row: RaceTrendRunningStyleRow;
  scores: Map<string, number | null>;
}

const isFiniteScoreValue = (value: number | null | undefined): value is number =>
  typeof value === "number" && Number.isFinite(value);

const averageScoreValues = (values: number[]): number =>
  values.reduce((acc, value) => acc + value, 0) / values.length;

const formatScore = (params: FormatScoreParams): string => {
  const values = params.row.targetHorseNumbers
    .map((umaban) => params.scores.get(umaban) ?? null)
    .filter(isFiniteScoreValue);
  if (values.length === 0) return SCORE_PLACEHOLDER;
  return averageScoreValues(values).toFixed(SCORE_DECIMAL_PLACES);
};

const sortDetailsByLatestRace = (details: RaceTrendDetail[]): RaceTrendDetail[] =>
  details.toSorted((a, b) => {
    const dateOrder = b.date.localeCompare(a.date);
    if (dateOrder !== 0) {
      return dateOrder;
    }
    const raceOrder = b.raceNumber.localeCompare(a.raceNumber, "ja", { numeric: true });
    if (raceOrder !== 0) {
      return raceOrder;
    }
    return (a.horseNumber ?? "").localeCompare(b.horseNumber ?? "", "ja", { numeric: true });
  });

interface RowSortContext {
  scores: Map<string, number | null>;
}

const averageRowScore = (
  row: RaceTrendRunningStyleRow,
  scores: Map<string, number | null>,
): number => {
  const values = row.targetHorseNumbers
    .map((umaban) => scores.get(umaban) ?? null)
    .filter(isFiniteScoreValue);
  // Rows without any finite score are pushed to the bottom of a descending sort.
  if (values.length === 0) return Number.NEGATIVE_INFINITY;
  return averageScoreValues(values);
};

const SORT_VALUE_GETTERS: Record<
  RaceTrendSortKey,
  (row: RaceTrendRunningStyleRow, context: RowSortContext) => number
> = {
  score: (row, context) => averageRowScore(row, context.scores),
  showRate: (row) => row.showRate,
  quinellaRate: (row) => row.quinellaRate,
  winRate: (row) => row.winRate,
};

interface RowComparatorParams {
  sortBy: RaceTrendSortKey;
  scores: Map<string, number | null>;
}

const compareRowsBySortKey =
  (params: RowComparatorParams) =>
  (a: RaceTrendRunningStyleRow, b: RaceTrendRunningStyleRow): number => {
    const getter = SORT_VALUE_GETTERS[params.sortBy];
    const context: RowSortContext = { scores: params.scores };
    const valueOrder = getter(b, context) - getter(a, context);
    if (valueOrder !== 0) return valueOrder;
    return a.key.localeCompare(b.key);
  };

const RACE_TREND_SORT_KEY_SET: Set<string> = new Set(RACE_TREND_SORT_KEYS);

const isRaceTrendSortKeyValue = (value: string): value is RaceTrendSortKey =>
  RACE_TREND_SORT_KEY_SET.has(value);

const parseSortChangeEvent = (value: string): RaceTrendSortKey =>
  isRaceTrendSortKeyValue(value) ? value : DEFAULT_RACE_TREND_SORT_KEY;

const deriveScoreConditionTrendTargets = (
  scoreConditions: RaceTrendScoreConditionsQuery,
): RaceTrendTargets => ({
  frame: scoreConditions.frame || scoreConditions.frameRunningStyle,
  jockey: scoreConditions.jockey,
  runningStyle: scoreConditions.frameRunningStyle,
  raceNumber: false,
});

const hasAnyScoreCondition = (scoreConditions: RaceTrendScoreConditionsQuery): boolean =>
  RACE_TREND_SCORE_CONDITION_QUERY_KEYS.some((key) => scoreConditions[key]);

const collectScoreDetailsForRow = (
  row: RaceTrendRunningStyleRow,
  scoreRows: RaceTrendRunningStyleRow[],
): RaceTrendDetail[] => {
  const targetSet: Set<string> = new Set(row.targetHorseNumbers);
  const seen: Set<string> = new Set();
  const overlapping = scoreRows.filter((scoreRow) =>
    scoreRow.targetHorseNumbers.some((umaban) => targetSet.has(umaban)),
  );
  return overlapping.flatMap((scoreRow) =>
    scoreRow.details.filter((detail) => {
      const key = `${detail.source}:${detail.date}:${detail.keibajoCode}:${detail.raceNumber}:${detail.horseNumber ?? ""}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    }),
  );
};

const getApiPath = ({
  day,
  defaultEndDate,
  keibajoCode,
  minStartDate,
  month,
  raceNumber,
  source,
  year,
}: Pick<
  RaceTrendSectionProps,
  | "day"
  | "defaultEndDate"
  | "keibajoCode"
  | "minStartDate"
  | "month"
  | "raceNumber"
  | "source"
  | "year"
>): string => {
  const params = new URLSearchParams({
    source,
    jockeyStart: minStartDate,
    jockeyEnd: defaultEndDate,
    frameStart: minStartDate,
    frameEnd: defaultEndDate,
    includeRealtimeResults: "true",
  });
  return `/api/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/trends?${params.toString()}`;
};

const normalizeYmd = (value: string): string => value.replaceAll("-", "");

const getLivePath = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  source,
  year,
}: Pick<
  RaceTrendSectionProps,
  "day" | "keibajoCode" | "month" | "raceNumber" | "source" | "year"
>): string => {
  const params = new URLSearchParams({ source });
  return `/api/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/trends/live?${params.toString()}`;
};

const getWebSocketUrl = (path: string): string => getRaceTrendLiveUrl(path);

const getRefreshedApiPath = (path: string): string => {
  const url = new URL(path, window.location.href);
  url.searchParams.set(RACE_TREND_CACHE_REFRESH_PARAM, "1");
  url.searchParams.set("_", String(Date.now()));
  return `${url.pathname}?${url.searchParams.toString()}`;
};

const getCurrentLocationPath = (): string =>
  `${window.location.pathname}${window.location.search}${window.location.hash}`;

const replaceRaceTrendTargetQuery = (trendTargets: RaceTrendTargets): void => {
  const url = new URL(window.location.href);
  clearRaceTrendTargetQueryParams(url.searchParams);
  if (!isDefaultRaceTrendTargets(trendTargets)) {
    url.searchParams.set(RACE_TREND_TARGET_QUERY_PARAM, serializeRaceTrendTargets(trendTargets));
  }
  const nextPath = `${url.pathname}${url.search}${url.hash}`;
  if (nextPath !== getCurrentLocationPath()) {
    window.history.replaceState(window.history.state, "", nextPath);
  }
};

const replaceRaceTrendScoreConditionsQuery = (
  scoreConditions: RaceTrendScoreConditionsQuery,
): void => {
  const url = new URL(window.location.href);
  clearRaceTrendScoreConditionsQueryParam(url.searchParams);
  if (!isDefaultRaceTrendScoreConditionsQuery(scoreConditions)) {
    url.searchParams.set(
      RACE_TREND_SCORE_CONDITIONS_QUERY_PARAM,
      serializeRaceTrendScoreConditionsQuery(scoreConditions),
    );
  }
  const nextPath = `${url.pathname}${url.search}${url.hash}`;
  if (nextPath !== getCurrentLocationPath()) {
    window.history.replaceState(window.history.state, "", nextPath);
  }
};

const replaceRaceTrendSortKeyQuery = (sortKey: RaceTrendSortKey): void => {
  const url = new URL(window.location.href);
  clearRaceTrendSortKeyQueryParam(url.searchParams);
  if (!isDefaultRaceTrendSortKey(sortKey)) {
    url.searchParams.set(RACE_TREND_SORT_QUERY_PARAM, serializeRaceTrendSortKeyQuery(sortKey));
  }
  const nextPath = `${url.pathname}${url.search}${url.hash}`;
  if (nextPath !== getCurrentLocationPath()) {
    window.history.replaceState(window.history.state, "", nextPath);
  }
};

const isRaceTrendUpdatedMessage = (value: unknown): boolean =>
  typeof value === "object" &&
  value !== null &&
  (value as { type?: unknown }).type === "trend-updated";

const TrendHeaderLabel = ({ children, secondLine }: { children: string; secondLine?: string }) => (
  <span className={secondLine ? "race-trend-header-label two-line" : "race-trend-header-label"}>
    <span>{children}</span>
    {secondLine ? <span>{secondLine}</span> : null}
  </span>
);

interface TooltipPosition {
  top: number;
  left: number;
}

const computeTooltipPosition = (rect: DOMRect): TooltipPosition => ({
  top: rect.bottom + RACE_TREND_TOOLTIP_OFFSET_PX,
  left: rect.left,
});

interface ScoreTooltipProps {
  anchorRef: React.RefObject<HTMLButtonElement | null>;
  isVisible: boolean;
}

function ScoreTooltipPortal({ anchorRef, isVisible }: ScoreTooltipProps) {
  const [mounted, setMounted] = useState(false);
  const [position, setPosition] = useState<TooltipPosition | null>(null);

  useEffect(() => {
    setMounted(true);
  }, []);

  useEffect(() => {
    if (!mounted) return undefined;
    const refresh = () => {
      const anchor = anchorRef.current;
      if (!anchor) {
        setPosition(null);
        return;
      }
      setPosition(computeTooltipPosition(anchor.getBoundingClientRect()));
    };
    refresh();
    window.addEventListener("scroll", refresh, true);
    window.addEventListener("resize", refresh);
    return () => {
      window.removeEventListener("scroll", refresh, true);
      window.removeEventListener("resize", refresh);
    };
  }, [mounted, anchorRef, isVisible]);

  const tooltipClassName = isVisible
    ? "race-trend-score-tooltip tooltip-visible"
    : "race-trend-score-tooltip";
  const style: React.CSSProperties =
    position === null ? { top: 0, left: 0 } : { top: position.top, left: position.left };
  const content = (
    <small
      className={tooltipClassName}
      id={RACE_TREND_SCORE_TOOLTIP_ID}
      role="tooltip"
      style={style}
    >
      {RACE_TREND_SCORE_TOOLTIP_TEXT}
    </small>
  );

  if (!mounted) return content;
  return createPortal(content, document.body);
}

interface RaceTrendTableProps {
  isLoading: boolean;
  raceCount: number;
  rows: RaceTrendRunningStyleRow[];
  scoreRows: RaceTrendRunningStyleRow[];
  scoreConditions: RaceTrendScoreConditionsQuery;
  trendTargets: RaceTrendTargets;
  umabanScores: Map<string, number | null>;
}

function RaceTrendTable({
  isLoading,
  raceCount,
  rows,
  scoreRows,
  scoreConditions,
  trendTargets,
  umabanScores,
}: RaceTrendTableProps) {
  const [expandedKey, setExpandedKey] = useState<string | null>(null);
  const [expandedScoreKey, setExpandedScoreKey] = useState<string | null>(null);
  const [scoreTooltipOpen, setScoreTooltipOpen] = useState(false);
  const [scoreTooltipHover, setScoreTooltipHover] = useState(false);
  const scoreHeaderRef = useRef<HTMLButtonElement | null>(null);
  const realtimePayload = useRealtimeRaceSelector((state) => state.payload);
  const realtimeOddsByHorse = useMemo(
    () =>
      new Map(
        (realtimePayload?.odds?.latest.tansho ?? []).map((row) => [
          normalizeHorseNumber(row.combination),
          { popularity: row.rank ?? null, winOdds: row.odds ?? null },
        ]),
      ),
    [realtimePayload],
  );
  const colSpan =
    9 +
    (trendTargets.frame ? 1 : 0) +
    (trendTargets.runningStyle ? 1 : 0) +
    (trendTargets.jockey ? 1 : 0) +
    (trendTargets.raceNumber ? 1 : 0);
  const effectiveExpandedKey = rows.some((row) => row.key === expandedKey) ? expandedKey : null;
  const effectiveExpandedScoreKey = rows.some((row) => row.key === expandedScoreKey)
    ? expandedScoreKey
    : null;
  const scoreClickable = hasAnyScoreCondition(scoreConditions);
  const tooltipVisible = scoreTooltipOpen || scoreTooltipHover;

  const openHorseDetail = (key: string, currentlyOpen: boolean) => {
    setExpandedScoreKey(null);
    setExpandedKey(currentlyOpen ? null : key);
  };

  const openScoreDetail = (key: string, currentlyOpen: boolean) => {
    setExpandedKey(null);
    setExpandedScoreKey(currentlyOpen ? null : key);
  };

  return (
    <div className="race-trend-table-panel">
      <div className="race-trend-subheading">
        <h3>脚質・枠・騎手ごとの勝率</h3>
        <span>集計 {raceCount}レース</span>
      </div>
      <div className="stats-table-wrap">
        <table className="stats-table race-trend-table aggregate">
          <colgroup>
            <col className="race-trend-col-horse-number" />
            {trendTargets.frame ? <col className="race-trend-col-frame" /> : null}
            {trendTargets.runningStyle ? <col className="race-trend-col-running-style" /> : null}
            {trendTargets.jockey ? <col className="race-trend-col-jockey" /> : null}
            {trendTargets.raceNumber ? <col className="race-trend-col-race-number" /> : null}
            <col className="race-trend-col-score" />
            <col className="race-trend-col-rate" />
            <col className="race-trend-col-rate" />
            <col className="race-trend-col-rate" />
            <col className="race-trend-col-market" />
            <col className="race-trend-col-market" />
            <col className="race-trend-col-count" />
            <col className="race-trend-col-median" />
          </colgroup>
          <thead>
            <tr>
              <th>
                <TrendHeaderLabel>馬番</TrendHeaderLabel>
              </th>
              {trendTargets.frame ? (
                <th>
                  <TrendHeaderLabel>枠</TrendHeaderLabel>
                </th>
              ) : null}
              {trendTargets.runningStyle ? (
                <th>
                  <TrendHeaderLabel>脚質</TrendHeaderLabel>
                </th>
              ) : null}
              {trendTargets.jockey ? (
                <th>
                  <TrendHeaderLabel>騎手</TrendHeaderLabel>
                </th>
              ) : null}
              {trendTargets.raceNumber ? (
                <th>
                  <TrendHeaderLabel>R</TrendHeaderLabel>
                </th>
              ) : null}
              <th>
                <button
                  aria-describedby={RACE_TREND_SCORE_TOOLTIP_ID}
                  aria-expanded={scoreTooltipOpen}
                  className={`race-trend-score-header${scoreTooltipOpen ? " tooltip-open" : ""}`}
                  onBlur={() => setScoreTooltipHover(false)}
                  onClick={() => setScoreTooltipOpen((value) => !value)}
                  onFocus={() => setScoreTooltipHover(true)}
                  onMouseEnter={() => setScoreTooltipHover(true)}
                  onMouseLeave={() => setScoreTooltipHover(false)}
                  ref={scoreHeaderRef}
                  type="button"
                >
                  <span>スコア</span>
                </button>
                <ScoreTooltipPortal anchorRef={scoreHeaderRef} isVisible={tooltipVisible} />
              </th>
              <th>
                <TrendHeaderLabel>複勝率</TrendHeaderLabel>
              </th>
              <th>
                <TrendHeaderLabel>連対率</TrendHeaderLabel>
              </th>
              <th>
                <TrendHeaderLabel>勝率</TrendHeaderLabel>
              </th>
              <th>
                <TrendHeaderLabel>人気</TrendHeaderLabel>
              </th>
              <th>
                <TrendHeaderLabel>単勝</TrendHeaderLabel>
              </th>
              <th>
                <TrendHeaderLabel>出走回数</TrendHeaderLabel>
              </th>
              <th>
                <TrendHeaderLabel secondLine="中央値">着順</TrendHeaderLabel>
              </th>
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              Array.from({ length: 5 }, (_, index) => (
                <tr className="race-trend-skeleton-row" key={`race-trend-skeleton-${index}`}>
                  <td>
                    <span className="race-trend-skeleton race-trend-skeleton-count" />
                  </td>
                  {trendTargets.frame ? (
                    <td>
                      <span className="race-trend-skeleton race-trend-skeleton-count" />
                    </td>
                  ) : null}
                  {trendTargets.runningStyle ? (
                    <td>
                      <span className="race-trend-skeleton race-trend-skeleton-count" />
                    </td>
                  ) : null}
                  {trendTargets.jockey ? (
                    <td>
                      <span className="race-trend-skeleton race-trend-skeleton-name" />
                    </td>
                  ) : null}
                  {trendTargets.raceNumber ? (
                    <td>
                      <span className="race-trend-skeleton race-trend-skeleton-count" />
                    </td>
                  ) : null}
                  <td className="race-trend-score-cell">
                    <span className="race-trend-skeleton race-trend-skeleton-count" />
                  </td>
                  <td>
                    <span className="race-trend-skeleton race-trend-skeleton-rate" />
                  </td>
                  <td>
                    <span className="race-trend-skeleton race-trend-skeleton-rate" />
                  </td>
                  <td>
                    <span className="race-trend-skeleton race-trend-skeleton-rate" />
                  </td>
                  <td>
                    <span className="race-trend-skeleton race-trend-skeleton-count" />
                  </td>
                  <td>
                    <span className="race-trend-skeleton race-trend-skeleton-rate" />
                  </td>
                  <td>
                    <span className="race-trend-skeleton race-trend-skeleton-count" />
                  </td>
                  <td>
                    <span className="race-trend-skeleton race-trend-skeleton-rate" />
                  </td>
                </tr>
              ))
            ) : rows.length > 0 ? (
              rows.map((row) => {
                const isExpanded = effectiveExpandedKey === row.key;
                const isScoreExpanded = effectiveExpandedScoreKey === row.key;
                return (
                  <RowFragment
                    isExpanded={isExpanded}
                    isScoreExpanded={isScoreExpanded}
                    key={row.key}
                    row={row}
                    realtimeOdds={realtimeOddsByHorse.get(
                      normalizeHorseNumber(row.targetHorseNumbers[0]),
                    )}
                    scoreClickable={scoreClickable}
                    scoreDetails={isScoreExpanded ? collectScoreDetailsForRow(row, scoreRows) : []}
                    trendTargets={trendTargets}
                    umabanScores={umabanScores}
                    onToggle={() => openHorseDetail(row.key, isExpanded)}
                    onToggleScore={() => openScoreDetail(row.key, isScoreExpanded)}
                  />
                );
              })
            ) : (
              <tr>
                <td className="race-trend-empty-cell" colSpan={colSpan}>
                  該当する集計成績はありません
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

interface RowFragmentProps {
  isExpanded: boolean;
  isScoreExpanded: boolean;
  realtimeOdds?: { popularity: number | null; winOdds: number | null };
  row: RaceTrendRunningStyleRow;
  scoreClickable: boolean;
  scoreDetails: RaceTrendDetail[];
  trendTargets: RaceTrendTargets;
  umabanScores: Map<string, number | null>;
  onToggle: () => void;
  onToggleScore: () => void;
}

function RowFragment({
  isExpanded,
  isScoreExpanded,
  realtimeOdds,
  row,
  scoreClickable,
  scoreDetails,
  trendTargets,
  umabanScores,
  onToggle,
  onToggleScore,
}: RowFragmentProps) {
  const colSpan =
    9 +
    (trendTargets.frame ? 1 : 0) +
    (trendTargets.runningStyle ? 1 : 0) +
    (trendTargets.jockey ? 1 : 0) +
    (trendTargets.raceNumber ? 1 : 0);
  const detailRows = useMemo(() => sortDetailsByLatestRace(row.details), [row.details]);
  const scoreDetailRows = useMemo(() => sortDetailsByLatestRace(scoreDetails), [scoreDetails]);
  const scoreValue = formatScore({ row, scores: umabanScores });
  const scoreIsClickable = scoreClickable && scoreValue !== SCORE_PLACEHOLDER;

  return (
    <>
      <tr className={isExpanded || isScoreExpanded ? "stats-row-expanded" : undefined}>
        <td className="race-trend-horse-number-cell">
          <button
            aria-expanded={isExpanded}
            className="stats-detail-toggle race-trend-detail-toggle"
            onClick={onToggle}
            title={
              trendTargets.runningStyle ? `${formatRunningStyle(row.runningStyle)}の詳細` : "詳細"
            }
            type="button"
          >
            <span>{row.targetHorseNumbers.join(",") || "-"}</span>
          </button>
        </td>
        {trendTargets.frame ? <td>{row.frameNumber ?? "-"}</td> : null}
        {trendTargets.runningStyle ? <td>{formatRunningStyle(row.runningStyle)}</td> : null}
        {trendTargets.jockey ? <td className="stats-name-cell">{row.jockeyName ?? "-"}</td> : null}
        {trendTargets.raceNumber ? <td>{formatRaceNumber(row.raceNumber)}</td> : null}
        <td className="race-trend-score-cell">
          {scoreIsClickable ? (
            <button
              aria-expanded={isScoreExpanded}
              className="race-trend-score-detail-toggle"
              onClick={(event) => {
                event.stopPropagation();
                onToggleScore();
              }}
              type="button"
            >
              {scoreValue}
            </button>
          ) : (
            <span>{scoreValue}</span>
          )}
        </td>
        <td>{formatRate(row.showRate)}</td>
        <td>{formatRate(row.quinellaRate)}</td>
        <td>{formatRate(row.winRate)}</td>
        <td>{formatMedian(realtimeOdds?.popularity)}</td>
        <td>{formatTrendWinOdds(realtimeOdds?.winOdds)}</td>
        <td>{row.starts}</td>
        <td>{formatMedian(row.finishPositionMedian)}</td>
      </tr>
      {isExpanded ? (
        <tr className="stats-detail-row">
          <td colSpan={colSpan}>
            <div className="stats-detail-panel">
              <table className="stats-detail-table race-trend-detail-table aggregate">
                <colgroup>
                  <col className="race-trend-detail-col-date" />
                  <col className="race-trend-detail-col-venue" />
                  <col className="race-trend-detail-col-race-number" />
                  <col className="race-trend-detail-col-horse-number" />
                  <col className="race-trend-detail-col-frame" />
                  <col className="race-trend-detail-col-running-style" />
                  <col className="race-trend-detail-col-jockey" />
                  <col className="race-trend-detail-col-finish" />
                  <col className="race-trend-detail-col-popularity" />
                  <col className="race-trend-detail-col-odds" />
                  <col className="race-trend-detail-col-horse-weight" />
                  <col className="race-trend-detail-col-horse-name" />
                  <col className="race-trend-detail-col-race-name" />
                </colgroup>
                <thead>
                  <tr>
                    <th>日付</th>
                    <th>場</th>
                    <th>R</th>
                    <th>馬番</th>
                    <th>枠</th>
                    <th>脚質</th>
                    <th>騎手</th>
                    <th>着順</th>
                    <th>人気</th>
                    <th>単勝</th>
                    <th>馬体重</th>
                    <th>馬名</th>
                    <th>レース名</th>
                  </tr>
                </thead>
                <tbody>
                  {detailRows.map((detail) => (
                    <tr
                      key={`${detail.source}:${detail.date}:${detail.keibajoCode}:${detail.raceNumber}:${detail.horseNumber}:${row.key}`}
                    >
                      <td>{detail.date}</td>
                      <td>{formatKeibajo(detail.keibajoCode)}</td>
                      <td>{formatRaceNumber(detail.raceNumber)}</td>
                      <td>{detail.horseNumber ?? "-"}</td>
                      <td>{detail.frameNumber ?? "-"}</td>
                      <td>{formatRunningStyle(detail.runningStyle)}</td>
                      <td>{detail.jockeyName ?? "-"}</td>
                      <td>{detail.finishPosition}</td>
                      <td>{formatMedian(detail.popularity)}</td>
                      <td>{formatTrendWinOdds(detail.winOdds)}</td>
                      <td>{formatHorseWeight(detail.horseWeight, detail.horseWeightDelta)}</td>
                      <td className="race-trend-detail-horse-name">{detail.horseName ?? "-"}</td>
                      <td className="race-trend-detail-race-name">{detail.raceName ?? "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </td>
        </tr>
      ) : null}
      {isScoreExpanded ? (
        <tr className="stats-detail-row race-trend-score-detail-row">
          <td colSpan={colSpan}>
            <div className="stats-detail-panel">
              <p className="race-trend-score-detail-heading">{SCORE_DETAIL_HEADING}</p>
              {scoreDetailRows.length === 0 ? (
                <p className="race-trend-empty-cell">{SCORE_DETAIL_EMPTY_MESSAGE}</p>
              ) : (
                <table className="stats-detail-table race-trend-detail-table aggregate">
                  <colgroup>
                    <col className="race-trend-detail-col-date" />
                    <col className="race-trend-detail-col-venue" />
                    <col className="race-trend-detail-col-race-number" />
                    <col className="race-trend-detail-col-horse-number" />
                    <col className="race-trend-detail-col-frame" />
                    <col className="race-trend-detail-col-running-style" />
                    <col className="race-trend-detail-col-jockey" />
                    <col className="race-trend-detail-col-finish" />
                    <col className="race-trend-detail-col-popularity" />
                    <col className="race-trend-detail-col-odds" />
                    <col className="race-trend-detail-col-horse-weight" />
                    <col className="race-trend-detail-col-horse-name" />
                    <col className="race-trend-detail-col-race-name" />
                  </colgroup>
                  <thead>
                    <tr>
                      <th>日付</th>
                      <th>場</th>
                      <th>R</th>
                      <th>馬番</th>
                      <th>枠</th>
                      <th>脚質</th>
                      <th>騎手</th>
                      <th>着順</th>
                      <th>人気</th>
                      <th>単勝</th>
                      <th>馬体重</th>
                      <th>馬名</th>
                      <th>レース名</th>
                    </tr>
                  </thead>
                  <tbody>
                    {scoreDetailRows.map((detail) => (
                      <tr
                        key={`score:${detail.source}:${detail.date}:${detail.keibajoCode}:${detail.raceNumber}:${detail.horseNumber}:${row.key}`}
                      >
                        <td>{detail.date}</td>
                        <td>{formatKeibajo(detail.keibajoCode)}</td>
                        <td>{formatRaceNumber(detail.raceNumber)}</td>
                        <td>{detail.horseNumber ?? "-"}</td>
                        <td>{detail.frameNumber ?? "-"}</td>
                        <td>{formatRunningStyle(detail.runningStyle)}</td>
                        <td>{detail.jockeyName ?? "-"}</td>
                        <td>{detail.finishPosition}</td>
                        <td>{formatMedian(detail.popularity)}</td>
                        <td>{formatTrendWinOdds(detail.winOdds)}</td>
                        <td>{formatHorseWeight(detail.horseWeight, detail.horseWeightDelta)}</td>
                        <td className="race-trend-detail-horse-name">{detail.horseName ?? "-"}</td>
                        <td className="race-trend-detail-race-name">{detail.raceName ?? "-"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </td>
        </tr>
      ) : null}
    </>
  );
}

export function RaceTrendSection({
  day,
  defaultEndDate,
  defaultStartDate,
  initialScoreConditions = DEFAULT_RACE_TREND_SCORE_CONDITIONS_QUERY,
  initialTrendTargets = DEFAULT_RACE_TREND_TARGETS,
  keibajoCode,
  minStartDate,
  month,
  raceNumber,
  source,
  year,
}: RaceTrendSectionProps) {
  const [jockeySameVenue, setJockeySameVenue] = useState(true);
  const [trendStart, setTrendStart] = useState(defaultStartDate);
  const [trendEnd, setTrendEnd] = useState(defaultEndDate);
  const [trendTargets, setTrendTargets] = useState<RaceTrendTargets>(initialTrendTargets);
  const [scoreConditions, setScoreConditions] =
    useState<RaceTrendScoreConditionsQuery>(initialScoreConditions);
  const [sortBy, setSortBy] = useState<RaceTrendSortKey>(DEFAULT_RACE_TREND_SORT_KEY);
  const [rawPayload, setRawPayload] = useState<RaceTrendRawPayload | null>(null);
  const [status, setStatus] = useState<"idle" | "loading" | "error">("idle");
  const trendTargetsRef = useRef(initialTrendTargets);
  const scoreConditionsRef = useRef(initialScoreConditions);
  const sortByRef = useRef<RaceTrendSortKey>(DEFAULT_RACE_TREND_SORT_KEY);
  const fetchSequenceRef = useRef(0);
  const liveConnectedRef = useRef(false);

  const toggleTrendTarget = useCallback((key: RaceTrendTargetKey) => {
    const currentTargets = trendTargetsRef.current;
    const nextTargets = {
      ...currentTargets,
      [key]: !currentTargets[key],
    };
    trendTargetsRef.current = nextTargets;
    replaceRaceTrendTargetQuery(nextTargets);
    setTrendTargets(nextTargets);
  }, []);

  const toggleScoreCondition = useCallback((key: RaceTrendScoreConditionKey) => {
    const currentConditions = scoreConditionsRef.current;
    const nextConditions = {
      ...currentConditions,
      [key]: !currentConditions[key],
    };
    scoreConditionsRef.current = nextConditions;
    replaceRaceTrendScoreConditionsQuery(nextConditions);
    setScoreConditions(nextConditions);
  }, []);

  const updateSortBy = useCallback((nextSortBy: RaceTrendSortKey) => {
    sortByRef.current = nextSortBy;
    replaceRaceTrendSortKeyQuery(nextSortBy);
    setSortBy(nextSortBy);
  }, []);

  useEffect(() => {
    // Hydrate sort key from the URL once on mount so deep links work.
    const initialSortKey = getRaceTrendSortKeyFromSearchParams(
      new URLSearchParams(window.location.search),
    );
    if (initialSortKey !== sortByRef.current) {
      sortByRef.current = initialSortKey;
      setSortBy(initialSortKey);
    }
  }, []);

  useEffect(() => {
    const handlePopState = () => {
      const search = new URLSearchParams(window.location.search);
      const nextTargets = getRaceTrendTargetsFromSearchParams(search);
      const nextScoreConditions = getRaceTrendScoreConditionsFromSearchParams(search);
      const nextSortKey = getRaceTrendSortKeyFromSearchParams(search);
      setTrendTargets((current) => {
        if (isSameRaceTrendTargets(current, nextTargets)) {
          return current;
        }
        trendTargetsRef.current = nextTargets;
        return nextTargets;
      });
      setScoreConditions((current) => {
        if (isSameRaceTrendScoreConditionsQuery(current, nextScoreConditions)) {
          return current;
        }
        scoreConditionsRef.current = nextScoreConditions;
        return nextScoreConditions;
      });
      setSortBy((current) => {
        if (current === nextSortKey) return current;
        sortByRef.current = nextSortKey;
        return nextSortKey;
      });
    };
    window.addEventListener("popstate", handlePopState);
    return () => {
      window.removeEventListener("popstate", handlePopState);
    };
  }, []);

  const apiPath = useMemo(
    () =>
      getApiPath({
        day,
        defaultEndDate,
        keibajoCode,
        minStartDate,
        month,
        raceNumber,
        source,
        year,
      }),
    [day, defaultEndDate, keibajoCode, minStartDate, month, raceNumber, source, year],
  );

  const livePath = useMemo(
    () => getLivePath({ day, keibajoCode, month, raceNumber, source, year }),
    [day, keibajoCode, month, raceNumber, source, year],
  );

  const { runningStyleRows: rows, raceCount } = useMemo(() => {
    if (!rawPayload) {
      return { runningStyleRows: [] as RaceTrendRunningStyleRow[], raceCount: 0 };
    }
    return aggregateForTargets(
      {
        starterRows: rawPayload.starterRows,
        currentRunningStyles: rawPayload.currentRunningStyles,
        historicalRunningStyles: rawPayload.historicalRunningStyles,
        raceContext: rawPayload.raceContext,
        runners: rawPayload.runners,
      },
      trendTargets,
      jockeySameVenue,
      normalizeYmd(trendStart || defaultStartDate),
      normalizeYmd(trendEnd || defaultEndDate),
    );
  }, [
    rawPayload,
    trendTargets,
    jockeySameVenue,
    trendStart,
    trendEnd,
    defaultStartDate,
    defaultEndDate,
  ]);

  // Second aggregation: rows keyed by the user-selected score conditions so
  // that clicking the score cell can show the records driving that score.
  const scoreAggregationRows = useMemo<RaceTrendRunningStyleRow[]>(() => {
    if (!rawPayload) return [];
    if (!hasAnyScoreCondition(scoreConditions)) return [];
    return aggregateForTargets(
      {
        starterRows: rawPayload.starterRows,
        currentRunningStyles: rawPayload.currentRunningStyles,
        historicalRunningStyles: rawPayload.historicalRunningStyles,
        raceContext: rawPayload.raceContext,
        runners: rawPayload.runners,
      },
      deriveScoreConditionTrendTargets(scoreConditions),
      jockeySameVenue,
      normalizeYmd(trendStart || defaultStartDate),
      normalizeYmd(trendEnd || defaultEndDate),
    ).runningStyleRows;
  }, [
    rawPayload,
    scoreConditions,
    jockeySameVenue,
    trendStart,
    trendEnd,
    defaultStartDate,
    defaultEndDate,
  ]);

  const scoreSourceMaps = useMemo(() => {
    const currentRunningStyleMap = buildCurrentRunningStyleMap(
      rawPayload?.currentRunningStyles ?? [],
    );
    const runningStyleByStarterKey = buildHistoricalRunningStyleMap(
      rawPayload?.historicalRunningStyles ?? [],
    );
    return { currentRunningStyleMap, runningStyleByStarterKey };
  }, [rawPayload]);

  const umabanScores = useMemo<Map<string, number | null>>(() => {
    const runners = rawPayload?.runners ?? [];
    const starterRows = rawPayload?.starterRows ?? [];
    const contexts = runners
      .map((runner) => buildScoreContext(runner, scoreSourceMaps.currentRunningStyleMap))
      .filter(hasValidUmaban);
    const details = starterRows
      .map((row) =>
        buildScoreDetail({
          row,
          runningStyleByStarterKey: scoreSourceMaps.runningStyleByStarterKey,
        }),
      )
      .filter(hasFinishPosition);
    const raw = computeRawUmabanScores({
      contexts,
      details,
      conditions: scoreConditions,
    });
    return normalizeUmabanScores(raw);
  }, [rawPayload, scoreSourceMaps, scoreConditions]);

  const sortedRows = useMemo<RaceTrendRunningStyleRow[]>(
    () => rows.toSorted(compareRowsBySortKey({ sortBy, scores: umabanScores })),
    [rows, sortBy, umabanScores],
  );

  const refreshTrendRows = useCallback(
    async ({
      clearOnError,
      refreshCache,
      requestId,
      showLoading,
    }: {
      clearOnError: boolean;
      refreshCache: boolean;
      requestId: number;
      showLoading: boolean;
    }) => {
      if (showLoading) {
        setStatus("loading");
      }
      try {
        const response = await fetchWithRetry(
          refreshCache ? getRefreshedApiPath(apiPath) : apiPath,
          // Allow Cloudflare's edge cache to satisfy follow-up navigations
          // when the worker response carries `Cache-Control: public,
          // max-age=60`. The cache-busting `__trendCacheRefresh` query param
          // is added by `getRefreshedApiPath` for forced re-fetches.
          { credentials: "include" },
          RACE_TREND_RETRY_OPTIONS,
        );
        if (!response.ok) {
          throw new Error(`race trend api ${response.status}`);
        }
        const body: unknown = await response.json();
        if (!isRaceTrendRawPayload(body)) {
          throw new Error("invalid race trend payload");
        }
        if (fetchSequenceRef.current !== requestId) {
          return;
        }
        setRawPayload(body);
        setStatus("idle");
      } catch {
        if (fetchSequenceRef.current !== requestId) {
          return;
        }
        if (clearOnError) {
          setRawPayload(null);
          setStatus("error");
        }
      }
    },
    [apiPath],
  );

  useEffect(() => {
    const requestId = fetchSequenceRef.current + 1;
    fetchSequenceRef.current = requestId;
    void refreshTrendRows({
      clearOnError: true,
      refreshCache: false,
      requestId,
      showLoading: true,
    });
    return () => {
      if (fetchSequenceRef.current === requestId) {
        fetchSequenceRef.current += 1;
      }
    };
  }, [refreshTrendRows]);

  useEffect(() => {
    const socket = new WebSocket(getWebSocketUrl(livePath));
    socket.addEventListener("open", () => {
      liveConnectedRef.current = true;
    });
    socket.addEventListener("message", (event) => {
      const payload: unknown = (() => {
        try {
          return JSON.parse(String(event.data));
        } catch {
          return null;
        }
      })();
      if (!isRaceTrendUpdatedMessage(payload)) {
        return;
      }
      const requestId = fetchSequenceRef.current + 1;
      fetchSequenceRef.current = requestId;
      void refreshTrendRows({
        clearOnError: false,
        refreshCache: false,
        requestId,
        showLoading: false,
      });
    });
    socket.addEventListener("close", () => {
      liveConnectedRef.current = false;
    });
    socket.addEventListener("error", () => {
      liveConnectedRef.current = false;
      socket.close();
    });
    return () => {
      liveConnectedRef.current = false;
      socket.close();
    };
  }, [livePath, refreshTrendRows]);

  useEffect(() => {
    const refreshIfFallbackNeeded = () => {
      if (document.visibilityState === "hidden" || liveConnectedRef.current) {
        return;
      }
      const requestId = fetchSequenceRef.current + 1;
      fetchSequenceRef.current = requestId;
      void refreshTrendRows({
        clearOnError: false,
        refreshCache: true,
        requestId,
        showLoading: false,
      });
    };

    const timer = window.setInterval(refreshIfFallbackNeeded, RACE_TREND_AUTO_REFRESH_INTERVAL_MS);
    document.addEventListener("visibilitychange", refreshIfFallbackNeeded);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", refreshIfFallbackNeeded);
    };
  }, [refreshTrendRows]);

  return (
    <section className="race-trend-section">
      <div className="section-heading compact">
        <h2>レース傾向</h2>
        <span>
          {formatKeibajo(keibajoCode)} {source === "jra" ? "中央競馬" : "地方競馬"}
        </span>
      </div>

      <div className="race-trend-card">
        <div className="race-trend-controls">
          <label>
            <span>開始日</span>
            <input
              max={defaultEndDate}
              min={minStartDate}
              onChange={(event) =>
                setTrendStart(clampIsoDateToRange(event.target.value, minStartDate, defaultEndDate))
              }
              type="date"
              value={trendStart}
            />
          </label>
          <label>
            <span>終了日</span>
            <input
              max={defaultEndDate}
              min={minStartDate}
              onChange={(event) =>
                setTrendEnd(clampIsoDateToRange(event.target.value, minStartDate, defaultEndDate))
              }
              type="date"
              value={trendEnd}
            />
          </label>
          <label className="race-trend-checkbox">
            <input
              checked={jockeySameVenue}
              onChange={(event) => setJockeySameVenue(event.target.checked)}
              type="checkbox"
            />
            <span>同じ競馬場のみ</span>
          </label>
        </div>

        <div className="combined-score-targets race-trend-targets" aria-label="勝率条件">
          <fieldset>
            <legend>勝率条件</legend>
            {RACE_TREND_TARGET_KEYS.map((key) => (
              <label key={key}>
                <input
                  checked={trendTargets[key]}
                  type="checkbox"
                  onChange={() => toggleTrendTarget(key)}
                />
                <span>{TREND_TARGET_LABELS[key]}</span>
              </label>
            ))}
          </fieldset>
        </div>

        <div className="combined-score-targets race-trend-targets" aria-label="スコア条件">
          <fieldset>
            <legend>スコア条件</legend>
            {RACE_TREND_SCORE_CONDITION_QUERY_KEYS.map((key) => (
              <label key={key}>
                <input
                  checked={scoreConditions[key]}
                  type="checkbox"
                  onChange={() => toggleScoreCondition(key)}
                />
                <span>{SCORE_CONDITION_LABELS[key]}</span>
              </label>
            ))}
          </fieldset>
        </div>

        <div className="combined-score-targets race-trend-sort" aria-label="並び順">
          <fieldset>
            <legend>並び順</legend>
            <label>
              <span>並び順</span>
              <select
                onChange={(event) => updateSortBy(parseSortChangeEvent(event.target.value))}
                value={sortBy}
              >
                {RACE_TREND_SORT_KEYS.map((key) => (
                  <option key={key} value={key}>
                    {SORT_KEY_LABELS[key]}
                  </option>
                ))}
              </select>
            </label>
          </fieldset>
        </div>

        <RaceTrendTable
          isLoading={status === "loading"}
          raceCount={raceCount}
          rows={sortedRows}
          scoreConditions={scoreConditions}
          scoreRows={scoreAggregationRows}
          trendTargets={trendTargets}
          umabanScores={umabanScores}
        />
      </div>

      {status === "error" ? (
        <p className="race-trend-error">レース傾向を取得できませんでした。</p>
      ) : null}
    </section>
  );
}
