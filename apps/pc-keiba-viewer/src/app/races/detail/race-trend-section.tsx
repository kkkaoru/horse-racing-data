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
  clearRaceTrendScoreLinkQuery,
  clearRaceTrendSortKeyQueryParam,
  clearRaceTrendTargetQueryParams,
  DEFAULT_RACE_TREND_SCORE_LINK_TO_WIN_RATE,
  DEFAULT_RACE_TREND_SORT_KEY,
  DEFAULT_RACE_TREND_TARGETS,
  getRaceTrendScoreConditionsFromSearchParams,
  getRaceTrendScoreLinkFromSearchParams,
  getRaceTrendSortKeyFromSearchParams,
  getRaceTrendTargetsFromSearchParams,
  isDefaultRaceTrendScoreConditionsQuery,
  isDefaultRaceTrendScoreLinkToWinRate,
  isDefaultRaceTrendSortKey,
  isDefaultRaceTrendTargets,
  isSameRaceTrendScoreConditionsQuery,
  isSameRaceTrendTargets,
  RACE_TREND_SCORE_CONDITION_QUERY_KEYS,
  RACE_TREND_SCORE_CONDITIONS_QUERY_PARAM,
  RACE_TREND_SCORE_LINK_QUERY_PARAM,
  RACE_TREND_SORT_KEYS,
  RACE_TREND_SORT_QUERY_PARAM,
  RACE_TREND_TARGET_KEYS,
  RACE_TREND_TARGET_QUERY_PARAM,
  serializeRaceTrendScoreConditionsQuery,
  serializeRaceTrendScoreLinkQuery,
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
  DEFAULT_RACE_TREND_SCORE_CONDITIONS,
  normalizeUmabanScores,
  type RecordFilter,
  type ScoreDetailInput,
  type UmabanContext,
} from "../../../lib/race-trend-score";
import type {
  RaceTrendRunningStyleCache,
  RaceTrendStarterRow,
} from "horse-racing-realtime/race-trend-daily-track-types";
import type {
  RaceTrendCurrentRunningStyle,
  RaceTrendDetail,
  RaceTrendRawPayload,
  RaceTrendRunnerSummary,
  RaceTrendRunningStyle,
  RaceTrendRunningStyleRow,
} from "../../../lib/race-types";
import { useRealtimeRaceSelector } from "./realtime-client";

const RACE_TREND_RETRY_OPTIONS = {
  baseDelayMs: 300,
  maxAttempts: 4,
  maxDelayMs: 4000,
} as const;

const RACE_TREND_AUTO_REFRESH_INTERVAL_MS = 60_000;

// Empty-state copy. The "preparing" label is used when the panel renders 0 rows
// but the trends API succeeded — this is the "data fetched but no sibling
// races yet (e.g. 1R before start, or later race whose siblings haven't been
// resulted yet)" branch. Showing a manual-retry affordance keeps the UI alive
// even when the route segment + edge cache + DO would otherwise return zero
// rows: the user can force a `__trendCacheRefresh` fetch that skips the
// upstream Cache API and reads the latest DO + legacy data.
const RACE_TREND_EMPTY_LABEL = "成績データが揃うのを待っています";
const RACE_TREND_EMPTY_DETAIL =
  "確定後のレースから順に表示します。 数十秒で自動更新しますが、手動で再取得することもできます。";
const RACE_TREND_RETRY_LABEL = "再取得";
const RACE_TREND_ERROR_LABEL = "レース傾向を取得できませんでした。";
const RACE_TREND_ERROR_DETAIL = "通信エラーで再取得します。 手動で再試行することもできます。";
const RACE_TREND_STALE_LABEL = "最新化に失敗したため、 直近のデータを表示しています。";

interface RaceTrendSectionProps {
  day: string;
  defaultEndDate: string;
  defaultStartDate: string;
  initialLinkScoreToWinRate?: boolean;
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
const SCORE_CONDITIONS_LABEL = "スコア計算条件";
const SCORE_LINK_TO_WIN_RATE_LABEL = "集計範囲を勝率条件に連動";
const RACE_TREND_CONDITIONS_SUMMARY_LABEL = "条件を変更";

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

// Per-condition trend-target shape. Each score condition is computed in its own
// independent aggregation pass; results are OR-unioned at detail-display time
// so that e.g. (frame + jockey) shows records matching frame OR records
// matching jockey, never the AND intersection.
const SCORE_CONDITION_TREND_TARGETS: Record<RaceTrendScoreConditionKey, RaceTrendTargets> = {
  frame: { frame: true, runningStyle: false, jockey: false, raceNumber: false },
  jockey: { frame: false, runningStyle: false, jockey: true, raceNumber: false },
  frameRunningStyle: { frame: true, runningStyle: true, jockey: false, raceNumber: false },
};

const hasAnyScoreCondition = (scoreConditions: RaceTrendScoreConditionsQuery): boolean =>
  RACE_TREND_SCORE_CONDITION_QUERY_KEYS.some((key) => scoreConditions[key]);

const buildScoreDetailDedupKey = (detail: RaceTrendDetail): string =>
  `${detail.source}:${detail.date}:${detail.keibajoCode}:${detail.raceNumber}:${detail.horseNumber ?? ""}`;

// Build per-row UmabanContext list used to filter detail records under the
// "集計範囲を勝率条件に連動" mode. Each umaban in the row gets its own context;
// a detail passes if any context matches under `predicateMatchesWinRateForDetail`.
interface BuildRowContextsParams {
  currentRunningStyleMap: Map<string, RaceTrendRunningStyle>;
  row: RaceTrendRunningStyleRow;
  runnerByHorseNumber: Map<string, RaceTrendRunnerSummary>;
}

const buildRowContexts = (params: BuildRowContextsParams): UmabanContext[] =>
  params.row.targetHorseNumbers
    .map((umaban) => params.runnerByHorseNumber.get(umaban))
    .filter((runner): runner is RaceTrendRunnerSummary => runner !== undefined)
    .map((runner) => buildScoreContext(runner, params.currentRunningStyleMap));

// Predicate that mirrors `predicateMatchesWinRate` but operates on
// RaceTrendDetail (the public detail shape rendered in the score panel). The
// detail carries `jockeyName`, so we derive its jockeyKey lazily here.
interface PredicateMatchesWinRateForDetailParams {
  context: UmabanContext;
  detail: RaceTrendDetail;
  trendTargets: RaceTrendTargets;
}

const predicateMatchesWinRateForDetail = (
  params: PredicateMatchesWinRateForDetailParams,
): boolean => {
  const { context, detail, trendTargets } = params;
  if (trendTargets.frame && detail.frameNumber !== context.frameNumber) return false;
  if (trendTargets.jockey && resolveRowJockeyKey(detail.jockeyName) !== context.jockeyKey) {
    return false;
  }
  if (trendTargets.runningStyle && detail.runningStyle !== context.runningStyle) return false;
  return true;
};

interface CollectScoreDetailsForRowParams {
  linkScoreToWinRate: boolean;
  row: RaceTrendRunningStyleRow;
  rowContexts: UmabanContext[];
  scoreRowsByCondition: Map<RaceTrendScoreConditionKey, RaceTrendRunningStyleRow[]>;
  scoreConditions: RaceTrendScoreConditionsQuery;
  trendTargets: RaceTrendTargets;
}

// Union (OR) of detail records across all selected score conditions. Records
// are deduped by a stable identifier so the same past race only appears once
// even if it matches multiple conditions.
const collectScoreDetailsForRow = (params: CollectScoreDetailsForRowParams): RaceTrendDetail[] => {
  const {
    linkScoreToWinRate,
    row,
    rowContexts,
    scoreRowsByCondition,
    scoreConditions,
    trendTargets,
  } = params;
  const targetSet: Set<string> = new Set(row.targetHorseNumbers);
  const selectedConditions = RACE_TREND_SCORE_CONDITION_QUERY_KEYS.filter(
    (key) => scoreConditions[key],
  );
  const seen: Map<string, RaceTrendDetail> = new Map();
  const matchesAnyContext = (detail: RaceTrendDetail): boolean =>
    !linkScoreToWinRate ||
    rowContexts.some((context) =>
      predicateMatchesWinRateForDetail({ context, detail, trendTargets }),
    );
  selectedConditions
    .flatMap((key) => scoreRowsByCondition.get(key) ?? [])
    .filter((scoreRow) => scoreRow.targetHorseNumbers.some((umaban) => targetSet.has(umaban)))
    .flatMap((scoreRow) => scoreRow.details)
    .filter(matchesAnyContext)
    .forEach((detail) => {
      const key = buildScoreDetailDedupKey(detail);
      if (!seen.has(key)) seen.set(key, detail);
    });
  return Array.from(seen.values());
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

const updateRaceTrendScoreLinkQuery = (linked: boolean): void => {
  const url = new URL(window.location.href);
  clearRaceTrendScoreLinkQuery(url.searchParams);
  if (!isDefaultRaceTrendScoreLinkToWinRate(linked)) {
    url.searchParams.set(
      RACE_TREND_SCORE_LINK_QUERY_PARAM,
      serializeRaceTrendScoreLinkQuery(linked),
    );
  }
  const nextPath = `${url.pathname}${url.search}${url.hash}`;
  if (nextPath !== getCurrentLocationPath()) {
    window.history.replaceState(window.history.state, "", nextPath);
  }
};

// Predicate used to restrict score-record aggregation to past records that
// match the user-selected 勝率条件. If all flags are off, this collapses to a
// no-op (returns true everywhere) and the score becomes equivalent to an
// unfiltered aggregation. Note: trendTargets.raceNumber is intentionally ignored
// because UmabanContext does not carry a race number for the current race.
interface PredicateMatchesWinRateParams {
  context: UmabanContext;
  detail: ScoreDetailInput;
  trendTargets: RaceTrendTargets;
}

const predicateMatchesWinRate = (params: PredicateMatchesWinRateParams): boolean => {
  const { context, detail, trendTargets } = params;
  if (trendTargets.frame && detail.frameNumber !== context.frameNumber) return false;
  if (trendTargets.jockey && detail.jockeyKey !== context.jockeyKey) return false;
  if (trendTargets.runningStyle && detail.runningStyle !== context.runningStyle) return false;
  return true;
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
  currentRunningStyleMap: Map<string, RaceTrendRunningStyle>;
  isLoading: boolean;
  linkScoreToWinRate: boolean;
  onManualRefresh: () => void;
  raceCount: number;
  rows: RaceTrendRunningStyleRow[];
  runnerByHorseNumber: Map<string, RaceTrendRunnerSummary>;
  scoreRowsByCondition: Map<RaceTrendScoreConditionKey, RaceTrendRunningStyleRow[]>;
  scoreConditions: RaceTrendScoreConditionsQuery;
  trendTargets: RaceTrendTargets;
  umabanScores: Map<string, number | null>;
}

function RaceTrendTable({
  currentRunningStyleMap,
  isLoading,
  linkScoreToWinRate,
  onManualRefresh,
  raceCount,
  rows,
  runnerByHorseNumber,
  scoreRowsByCondition,
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
                const rowContexts = isScoreExpanded
                  ? buildRowContexts({ currentRunningStyleMap, row, runnerByHorseNumber })
                  : [];
                const scoreDetails = isScoreExpanded
                  ? collectScoreDetailsForRow({
                      linkScoreToWinRate,
                      row,
                      rowContexts,
                      scoreConditions,
                      scoreRowsByCondition,
                      trendTargets,
                    })
                  : [];
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
                    scoreDetails={scoreDetails}
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
                  <div className="race-trend-empty-state">
                    <p className="race-trend-empty-label">{RACE_TREND_EMPTY_LABEL}</p>
                    <p className="race-trend-empty-detail">{RACE_TREND_EMPTY_DETAIL}</p>
                    <button
                      className="race-trend-retry-button"
                      onClick={onManualRefresh}
                      type="button"
                    >
                      {RACE_TREND_RETRY_LABEL}
                    </button>
                  </div>
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
  initialLinkScoreToWinRate = DEFAULT_RACE_TREND_SCORE_LINK_TO_WIN_RATE,
  initialScoreConditions = DEFAULT_RACE_TREND_SCORE_CONDITIONS,
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
  const [linkScoreToWinRate, setLinkScoreToWinRate] = useState<boolean>(initialLinkScoreToWinRate);
  const [sortBy, setSortBy] = useState<RaceTrendSortKey>(DEFAULT_RACE_TREND_SORT_KEY);
  const [rawPayload, setRawPayload] = useState<RaceTrendRawPayload | null>(null);
  const [status, setStatus] = useState<"idle" | "loading" | "error">("idle");
  // Tracks whether the most-recent background refresh failed while the panel
  // still holds the previously-fetched payload. When true, we keep showing the
  // last-known rows but surface a non-blocking "再試行" banner so the user
  // knows the auto-refresh / WebSocket-driven update silently failed and can
  // manually retry without losing context.
  const [hasStaleRefresh, setHasStaleRefresh] = useState(false);
  const trendTargetsRef = useRef(initialTrendTargets);
  const scoreConditionsRef = useRef(initialScoreConditions);
  const linkScoreToWinRateRef = useRef<boolean>(initialLinkScoreToWinRate);
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

  const toggleScoreLinkToWinRate = useCallback(() => {
    const nextLinked = !linkScoreToWinRateRef.current;
    linkScoreToWinRateRef.current = nextLinked;
    updateRaceTrendScoreLinkQuery(nextLinked);
    setLinkScoreToWinRate(nextLinked);
  }, []);

  const updateSortBy = useCallback((nextSortBy: RaceTrendSortKey) => {
    sortByRef.current = nextSortBy;
    replaceRaceTrendSortKeyQuery(nextSortBy);
    setSortBy(nextSortBy);
  }, []);

  useEffect(() => {
    // Hydrate sort key and score-link flag from the URL once on mount so deep links work.
    const search = new URLSearchParams(window.location.search);
    const initialSortKey = getRaceTrendSortKeyFromSearchParams(search);
    if (initialSortKey !== sortByRef.current) {
      sortByRef.current = initialSortKey;
      setSortBy(initialSortKey);
    }
    const initialLinked = getRaceTrendScoreLinkFromSearchParams(search);
    if (initialLinked !== linkScoreToWinRateRef.current) {
      linkScoreToWinRateRef.current = initialLinked;
      setLinkScoreToWinRate(initialLinked);
    }
  }, []);

  useEffect(() => {
    const handlePopState = () => {
      const search = new URLSearchParams(window.location.search);
      const nextTargets = getRaceTrendTargetsFromSearchParams(search);
      const nextScoreConditions = getRaceTrendScoreConditionsFromSearchParams(search);
      const nextSortKey = getRaceTrendSortKeyFromSearchParams(search);
      const nextLinked = getRaceTrendScoreLinkFromSearchParams(search);
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
      setLinkScoreToWinRate((current) => {
        if (current === nextLinked) return current;
        linkScoreToWinRateRef.current = nextLinked;
        return nextLinked;
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

  // Per-condition aggregations: each selected score condition is aggregated in
  // isolation so the score-detail panel can union the results (OR) instead of
  // intersecting them (AND). Without this, ticking both 枠 and 騎手 would only
  // show records matching BOTH — the user wants records matching EITHER.
  const scoreAggregationByCondition = useMemo<
    Map<RaceTrendScoreConditionKey, RaceTrendRunningStyleRow[]>
  >(() => {
    const result: Map<RaceTrendScoreConditionKey, RaceTrendRunningStyleRow[]> = new Map();
    if (!rawPayload) return result;
    const startYmd = normalizeYmd(trendStart || defaultStartDate);
    const endYmd = normalizeYmd(trendEnd || defaultEndDate);
    const input = {
      starterRows: rawPayload.starterRows,
      currentRunningStyles: rawPayload.currentRunningStyles,
      historicalRunningStyles: rawPayload.historicalRunningStyles,
      raceContext: rawPayload.raceContext,
      runners: rawPayload.runners,
    };
    RACE_TREND_SCORE_CONDITION_QUERY_KEYS.filter((key) => scoreConditions[key]).forEach((key) => {
      const { runningStyleRows } = aggregateForTargets(
        input,
        SCORE_CONDITION_TREND_TARGETS[key],
        jockeySameVenue,
        startYmd,
        endYmd,
      );
      result.set(key, runningStyleRows);
    });
    return result;
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

  // Map current-race umaban → runner so the score-detail filter can build per-row
  // UmabanContext on demand when "集計範囲を勝率条件に連動" is enabled.
  const runnerByHorseNumber = useMemo<Map<string, RaceTrendRunnerSummary>>(
    () =>
      new Map(
        (rawPayload?.runners ?? [])
          .map((runner) => {
            const umaban = normalizeNumberText(runner.horseNumber);
            return umaban ? ([umaban, runner] satisfies [string, RaceTrendRunnerSummary]) : null;
          })
          .filter((entry): entry is [string, RaceTrendRunnerSummary] => entry !== null),
      ),
    [rawPayload],
  );

  // When the user opts in via the "集計範囲を勝率条件に連動" checkbox, restrict
  // each score record to those whose detail matches the active 勝率条件 (frame /
  // jockey / runningStyle). If all 勝率条件 flags are off, the predicate is a no-op
  // and the score is computed over all records (equivalent to undefined filter).
  const recordFilter = useMemo<RecordFilter | undefined>(() => {
    if (!linkScoreToWinRate) return undefined;
    return (params) =>
      predicateMatchesWinRate({
        context: params.context,
        detail: params.detail,
        trendTargets,
      });
  }, [linkScoreToWinRate, trendTargets]);

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
      recordFilter,
    });
    return normalizeUmabanScores(raw);
  }, [rawPayload, scoreSourceMaps, scoreConditions, recordFilter]);

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
        setHasStaleRefresh(false);
      } catch {
        if (fetchSequenceRef.current !== requestId) {
          return;
        }
        if (clearOnError) {
          setRawPayload(null);
          setStatus("error");
          setHasStaleRefresh(false);
          return;
        }
        // Preserve the previously-fetched payload but surface a "再試行" banner
        // so the user knows the background update silently failed.
        setHasStaleRefresh(true);
      }
    },
    [apiPath],
  );

  const requestManualRefresh = useCallback(() => {
    const requestId = fetchSequenceRef.current + 1;
    fetchSequenceRef.current = requestId;
    void refreshTrendRows({
      clearOnError: rawPayload === null,
      refreshCache: true,
      requestId,
      showLoading: true,
    });
  }, [rawPayload, refreshTrendRows]);

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
      // The trend-updated signal means a sibling race just resulted and the
      // upstream KV / Cache API entries have been busted. Force `refreshCache`
      // so the next fetch sets `__trendCacheRefresh=1` and skips any
      // edge-cached body that hasn't picked up the DO + legacy merge yet.
      void refreshTrendRows({
        clearOnError: false,
        refreshCache: true,
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

        <details className="race-trend-conditions-disclosure" open>
          <summary>{RACE_TREND_CONDITIONS_SUMMARY_LABEL}</summary>
          <div className="race-trend-conditions-disclosure-body">
            <div className="race-trend-conditions-grid">
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

              <div
                className="combined-score-targets race-trend-targets"
                aria-label={SCORE_CONDITIONS_LABEL}
              >
                <fieldset>
                  <legend>{SCORE_CONDITIONS_LABEL}</legend>
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
                  <label className="race-trend-score-link-toggle">
                    <input
                      checked={linkScoreToWinRate}
                      type="checkbox"
                      onChange={() => toggleScoreLinkToWinRate()}
                    />
                    <span>{SCORE_LINK_TO_WIN_RATE_LABEL}</span>
                  </label>
                </fieldset>
              </div>
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
          </div>
        </details>

        {hasStaleRefresh && status !== "error" ? (
          <div className="race-trend-stale-banner" role="status">
            <span>{RACE_TREND_STALE_LABEL}</span>
            <button
              className="race-trend-retry-button"
              onClick={requestManualRefresh}
              type="button"
            >
              {RACE_TREND_RETRY_LABEL}
            </button>
          </div>
        ) : null}

        <RaceTrendTable
          currentRunningStyleMap={scoreSourceMaps.currentRunningStyleMap}
          isLoading={status === "loading"}
          linkScoreToWinRate={linkScoreToWinRate}
          onManualRefresh={requestManualRefresh}
          raceCount={raceCount}
          rows={sortedRows}
          runnerByHorseNumber={runnerByHorseNumber}
          scoreConditions={scoreConditions}
          scoreRowsByCondition={scoreAggregationByCondition}
          trendTargets={trendTargets}
          umabanScores={umabanScores}
        />
      </div>

      {status === "error" ? (
        <div className="race-trend-error" role="alert">
          <p className="race-trend-error-label">{RACE_TREND_ERROR_LABEL}</p>
          <p className="race-trend-error-detail">{RACE_TREND_ERROR_DETAIL}</p>
          <button className="race-trend-retry-button" onClick={requestManualRefresh} type="button">
            {RACE_TREND_RETRY_LABEL}
          </button>
        </div>
      ) : null}
    </section>
  );
}
