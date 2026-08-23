"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from "react";

import type { RaceSource } from "../../../lib/codes";
import {
  cleanText,
  formatDate,
  formatDistance,
  formatKeibajo,
  formatRaceNumber,
  formatTrack,
  formatWeather,
  getTrackSurfaceLabel,
} from "../../../lib/format";
import { DEFAULT_SHOW_RESULTS_CHART } from "../../../lib/horse-race-time-charts";
import { getRaceTags } from "../../../lib/race-classification";
import {
  buildRacePacePredictionRowsFromResults,
  isCornerPacePredictionSupported,
  RACE_PACE_PREDICTION_RESULTS_EVENT,
} from "../../../lib/race-pace-prediction";
import type { HorseRaceResult, RaceTimeStats, Runner } from "../../../lib/race-types";
import { getRunnerDisplayNames } from "../../../lib/runner-display";
import {
  formatCarriedWeight,
  formatHorseWeight,
  formatRunnerNumber,
  formatRunnerValue,
  formatSexAge,
  isBanEiKeibajoCode,
} from "../../../lib/runner-format";
import {
  loadResultsChartForCurrentUser,
  persistResultsChartForCurrentUser,
} from "../../../lib/user-preferences-indexeddb";
import { FrameNumberBadge } from "./frame-number-badge";
import { HorseRaceTimeChart } from "./horse-race-time-charts";
import { MobileFilterDisclosure } from "./mobile-filter-disclosure";
import { RaceTimeStatsMetrics } from "./race-time-stats-metrics";

type ResultLimit = "all" | "1" | "3" | "5" | "10";
type SortDirection = "asc" | "desc";
type SortKey = "date" | "kohan3f" | "sohaTime";

interface ResultsViewToggleProps {
  showChart: boolean;
  onShowChart: (showChart: boolean) => void;
}

interface HorseRaceResultsTableProps {
  classConditionName: string | null;
  currentDistance: string | null | undefined;
  currentKeibajoCode: string;
  currentRaceDate: string;
  currentTrackCode: string | null;
  defaultIncludeClass: boolean;
  raceTimeStats: RaceTimeStats | null;
  results: HorseRaceResult[];
  runners: Runner[];
  source: RaceSource;
  sourceScope: RaceSource | "all";
}

interface ComparableRaceTimeContext {
  baseDistance: number;
  currentKeibajoCode: string;
  currentTrackCode: string | null;
  direction: SortDirection;
}

const DEFAULT_RECENT_MONTHS = 7;
const RECENT_MONTHS_RELAX_STEP = 2;
const RECENT_MONTHS_STEP = 2;
const RESULTS_VIEW_RADIO_NAME: string = "results-view";

const SORT_LABELS: Record<SortKey, string> = {
  date: "日付",
  kohan3f: "上がり3F",
  sohaTime: "レースタイム",
};

const COMPARABLE_SURFACE_LABELS = ["芝", "ダート", "サンド", "障害"] satisfies readonly string[];

const ResultsViewToggle = ({ onShowChart, showChart }: ResultsViewToggleProps) => (
  <fieldset aria-label="競走成績の表示" className="win-rate-heatmap-view-toggle">
    <label className="running-style-bucket-toggle-label">
      <input
        checked={showChart}
        name={RESULTS_VIEW_RADIO_NAME}
        type="radio"
        value="chart"
        onChange={() => {
          onShowChart(true);
        }}
      />
      グラフ
    </label>
    <label className="running-style-bucket-toggle-label">
      <input
        checked={!showChart}
        name={RESULTS_VIEW_RADIO_NAME}
        type="radio"
        value="text"
        onChange={() => {
          onShowChart(false);
        }}
      />
      テキスト
    </label>
  </fieldset>
);

const isResultLimit = (value: string): value is ResultLimit =>
  value === "all" || value === "1" || value === "3" || value === "5" || value === "10";

const toResultLimit = (value: string): ResultLimit => (isResultLimit(value) ? value : "all");

const parseNumber = (value: string | null | undefined): number | null => {
  const cleaned = cleanText(value, "");
  if (!cleaned || /^0+$/.test(cleaned)) {
    return null;
  }
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};

const parseSignedTenths = (value: string | null | undefined): number | null => {
  const cleaned = cleanText(value, "");
  if (!cleaned || /^[+-]?0+$/.test(cleaned)) {
    return null;
  }
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed / 10 : null;
};

const compareNullable = (
  left: number | null,
  right: number | null,
  direction: SortDirection,
): number => {
  if (left === null && right === null) {
    return 0;
  }
  if (left === null) {
    return 1;
  }
  if (right === null) {
    return -1;
  }
  return direction === "asc" ? left - right : right - left;
};

const compareMatchFlag = (leftMatched: boolean, rightMatched: boolean): number => {
  if (leftMatched === rightMatched) {
    return 0;
  }
  return leftMatched ? -1 : 1;
};

const isComparableSurfaceLabel = (label: string): boolean =>
  COMPARABLE_SURFACE_LABELS.some((surface) => surface === label);

const formatRaceName = (result: HorseRaceResult): string => {
  const names = [
    cleanText(result.kyosomeiHondai, ""),
    cleanText(result.kyosomeiFukudai, ""),
    cleanText(result.kyosomeiKakkonai, ""),
  ].filter(Boolean);
  return names.length > 0 ? names.join(" / ") : "一般競走";
};

const formatRaceConditions = (result: HorseRaceResult): string => {
  const tags = getRaceTags(result);
  return tags.length > 0 ? tags.join(" / ") : cleanText(result.kyosoJokenMeisho);
};

const formatTenthsTime = (value: string | null | undefined, decodeBanEi = false): string => {
  const cleaned = cleanText(value, "");
  const tenths = parseNumber(cleaned);
  if (tenths === null) {
    return "-";
  }
  if (decodeBanEi) {
    const padded = cleaned.padStart(4, "0");
    return `${Number(padded.slice(0, -3))}:${padded.slice(-3, -1)}.${padded.slice(-1)}`;
  }
  const minutes = Math.floor(tenths / 600);
  const seconds = Math.floor((tenths % 600) / 10);
  const remainder = tenths % 10;
  return minutes > 0
    ? `${minutes}:${String(seconds).padStart(2, "0")}.${remainder}`
    : `${seconds}.${remainder}`;
};

const formatDecimalTenths = (value: string | null | undefined): string => {
  const tenths = parseNumber(value);
  return tenths === null ? "-" : (tenths / 10).toFixed(1);
};

const formatTimeDifference = (value: string | null | undefined): string => {
  const difference = parseSignedTenths(value);
  if (difference === null) {
    return "-";
  }
  return difference > 0 ? `+${difference.toFixed(1)}` : difference.toFixed(1);
};

const formatOdds = (value: string | null | undefined): string => {
  const odds = parseNumber(value);
  return odds === null ? "-" : (odds / 10).toFixed(1);
};

const formatRank = (value: string | null | undefined): string => {
  const rank = parseNumber(value);
  return rank === null ? "-" : String(rank);
};

const formatCornerRank = (value: string | null | undefined): string | null => {
  const rank = parseNumber(value);
  return rank === null ? null : String(rank);
};

const formatCornerRanks = (result: HorseRaceResult): string => {
  const corners = [
    formatCornerRank(result.corner1),
    formatCornerRank(result.corner2),
    formatCornerRank(result.corner3),
    formatCornerRank(result.corner4),
  ].filter((rank): rank is string => rank !== null);
  return corners.length > 0 ? corners.join("-") : "-";
};

const normalizeText = (value: string | null | undefined): string =>
  cleanText(value, "").replace(/\s+/g, "").replace(/　+/g, "");

const normalizeConditionText = (value: string | null | undefined): string =>
  cleanText(value, "")
    .replace(/[Ａ-Ｚａ-ｚ０-９]/g, (char) => String.fromCharCode(char.charCodeAt(0) - 0xfee0))
    .replace(/[－ー―‐]/g, "-")
    .replace(/\s+/g, " ")
    .replace(/　+/g, " ")
    .trim();

const getRaceResultClassLabel = (result: HorseRaceResult): string => {
  const localClass = normalizeConditionText(result.kyosoJokenMeisho).split(" ")[0] ?? "";
  if (/^[A-Z][0-9]+(?:-[0-9]+)?$/.test(localClass)) {
    return localClass;
  }
  const tags = getRaceTags(result);
  return tags.find((tag) => /^[A-Z][0-9]+(?:-[0-9]+)?$/.test(tag)) ?? "";
};

const getClassFilterOptions = (classConditionName: string | null): string[] => {
  const normalized = normalizeConditionText(classConditionName);
  const match = normalized.match(/^([A-Z])([0-9]+)(?:-[0-9]+)?$/);
  if (!match) {
    return normalized ? [normalized] : [];
  }
  const [, alphabet, number] = match;
  if (!alphabet || !number) {
    return normalized ? [normalized] : [];
  }
  return [...new Set([alphabet, `${alphabet}${number}`, normalized])];
};

const isClassMatched = (result: HorseRaceResult, classFilter: string): boolean => {
  const resultClass = getRaceResultClassLabel(result);
  if (!resultClass) {
    return false;
  }
  if (/^[A-Z]$/.test(classFilter)) {
    return resultClass.startsWith(classFilter);
  }
  if (/^[A-Z][0-9]+$/.test(classFilter)) {
    return resultClass === classFilter || resultClass.startsWith(`${classFilter}-`);
  }
  return resultClass === classFilter;
};

const getRaceDateValue = (result: HorseRaceResult): number | null => {
  const raw = `${result.kaisaiNen}${result.kaisaiTsukihi}`;
  if (!/^\d{8}$/.test(raw)) {
    return null;
  }
  return Number(raw);
};

const getSortValue = (result: HorseRaceResult, key: SortKey): number | null => {
  if (key === "date") {
    return getRaceDateValue(result);
  }
  if (key === "kohan3f") {
    return parseNumber(result.kohan3f);
  }
  return parseSohaTimeTenths(result.sohaTime, isBanEiKeibajoCode(result.keibajoCode));
};

const getDistanceValue = (result: HorseRaceResult): number | null => parseNumber(result.kyori);

const getDateMonthsBefore = (date: string, months: number): number | null => {
  if (!/^\d{8}$/.test(date)) {
    return null;
  }
  const parsed = new Date(
    Number(date.slice(0, 4)),
    Number(date.slice(4, 6)) - 1,
    Number(date.slice(6, 8)),
  );
  parsed.setMonth(parsed.getMonth() - months);
  return Number(
    `${parsed.getFullYear()}${String(parsed.getMonth() + 1).padStart(2, "0")}${String(parsed.getDate()).padStart(2, "0")}`,
  );
};

const getMonthsBetweenRaceDates = (fromDate: string, toDate: string): number | null => {
  if (!/^\d{8}$/.test(fromDate) || !/^\d{8}$/.test(toDate)) {
    return null;
  }

  const fromYear = Number(fromDate.slice(0, 4));
  const fromMonth = Number(fromDate.slice(4, 6));
  const fromDay = Number(fromDate.slice(6, 8));
  const toYear = Number(toDate.slice(0, 4));
  const toMonth = Number(toDate.slice(4, 6));
  const toDay = Number(toDate.slice(6, 8));
  const monthDiff = (toYear - fromYear) * 12 + (toMonth - fromMonth);

  return monthDiff + (toDay > fromDay ? 1 : 0);
};

const getRunnerNumberOptions = (runners: Runner[], results: HorseRaceResult[]): string[] => {
  const runnerNumbers =
    runners.length > 0
      ? runners.map((runner) => cleanText(runner.umaban, ""))
      : results.map((result) => cleanText(result.currentUmaban, ""));
  return [...new Set(runnerNumbers.filter(Boolean))].toSorted(
    (left, right) => Number(left) - Number(right),
  );
};

const getCoveredRunnerNumbers = (results: HorseRaceResult[]): Set<string> =>
  new Set(results.map((result) => cleanText(result.currentUmaban, "")).filter(Boolean));

// Decode a soha time into comparable total-tenths so faster times compare smaller.
// Ban-ei encodes minutes/seconds/tenths packed in decimal (e.g. "3188" -> 3:18.8),
// while other tracks store the value directly in tenths.
const parseSohaTimeTenths = (
  value: string | null | undefined,
  decodeBanEi: boolean,
): number | null => {
  const raw = parseNumber(value);
  if (raw === null) {
    return null;
  }
  if (!decodeBanEi) {
    return raw;
  }
  const padded = cleanText(value, "").padStart(4, "0");
  const minutes = Number(padded.slice(0, -3));
  const seconds = Number(padded.slice(-3, -1));
  const tenths = Number(padded.slice(-1));
  return minutes * 600 + seconds * 10 + tenths;
};

// Distance difference relative to the target distance, null when distance is missing.
const getDistanceDiff = (result: HorseRaceResult, baseDistance: number): number | null => {
  const distance = getDistanceValue(result);
  if (distance === null || !Number.isFinite(baseDistance)) {
    return null;
  }
  return Math.abs(distance - baseDistance);
};

const getComparablePace = (result: HorseRaceResult): number | null => {
  const time = parseSohaTimeTenths(result.sohaTime, isBanEiKeibajoCode(result.keibajoCode));
  const distance = getDistanceValue(result);
  if (time === null || distance === null || distance <= 0) {
    return null;
  }
  return time / distance;
};

const compareRunnerNumbers = (left: HorseRaceResult, right: HorseRaceResult): number =>
  Number(left.currentUmaban ?? 0) - Number(right.currentUmaban ?? 0);

const compareEqualDistanceTimes = (
  left: HorseRaceResult,
  right: HorseRaceResult,
  direction: SortDirection,
): number => {
  const timeCompared = compareNullable(
    parseSohaTimeTenths(left.sohaTime, isBanEiKeibajoCode(left.keibajoCode)),
    parseSohaTimeTenths(right.sohaTime, isBanEiKeibajoCode(right.keibajoCode)),
    direction,
  );
  if (timeCompared !== 0) {
    return timeCompared;
  }
  const dateCompared = compareNullable(getRaceDateValue(left), getRaceDateValue(right), "desc");
  if (dateCompared !== 0) {
    return dateCompared;
  }
  return compareRunnerNumbers(left, right);
};

// Comparison-oriented race-time order for 競走成績:
// 1. Same surface as the current race (turf / dirt / sand / jumps clocks).
//    Surface is a sort key only unless the user turns on a filter.
// 2. Same distance as the current race first (apples-to-apples clocks).
// 3. Then closer distances.
// 4. Then the longer distance when the gap to the current race is equal.
// 5. Then the current venue, then the exact track code (turn / inner-outer).
//    Venue is a sort key only unless the user enables the same-venue filter.
// 6. Then faster/slower time (decoded tenths; ban-ei packed times included).
//    Different-distance rows use pace (time / distance) so a shorter trip is
//    not automatically "faster".
// 7. Then newer date, then horse number.
const createComparableRaceTimeComparator =
  ({ baseDistance, currentKeibajoCode, currentTrackCode, direction }: ComparableRaceTimeContext) =>
  (left: HorseRaceResult, right: HorseRaceResult): number => {
    const currentSurface = getTrackSurfaceLabel(currentTrackCode);
    if (isComparableSurfaceLabel(currentSurface)) {
      const surfaceCompared = compareMatchFlag(
        getTrackSurfaceLabel(left.trackCode) === currentSurface,
        getTrackSurfaceLabel(right.trackCode) === currentSurface,
      );
      if (surfaceCompared !== 0) {
        return surfaceCompared;
      }
    }
    const leftDistance = getDistanceValue(left);
    const rightDistance = getDistanceValue(right);
    const hasBase = Number.isFinite(baseDistance) && baseDistance > 0;
    if (hasBase) {
      const leftSame = leftDistance === baseDistance;
      const rightSame = rightDistance === baseDistance;
      if (leftSame !== rightSame) {
        return leftSame ? -1 : 1;
      }
      const distanceCompared = compareNullable(
        getDistanceDiff(left, baseDistance),
        getDistanceDiff(right, baseDistance),
        "asc",
      );
      if (distanceCompared !== 0) {
        return distanceCompared;
      }
      const longerCompared = compareNullable(leftDistance, rightDistance, "desc");
      if (longerCompared !== 0) {
        return longerCompared;
      }
    }
    const currentKeibajo = cleanText(currentKeibajoCode, "");
    if (currentKeibajo.length > 0) {
      const keibajoCompared = compareMatchFlag(
        cleanText(left.keibajoCode, "") === currentKeibajo,
        cleanText(right.keibajoCode, "") === currentKeibajo,
      );
      if (keibajoCompared !== 0) {
        return keibajoCompared;
      }
    }
    const currentTrack = cleanText(currentTrackCode, "");
    if (currentTrack.length > 0) {
      const trackCompared = compareMatchFlag(
        cleanText(left.trackCode, "") === currentTrack,
        cleanText(right.trackCode, "") === currentTrack,
      );
      if (trackCompared !== 0) {
        return trackCompared;
      }
    }
    if (leftDistance !== null && leftDistance === rightDistance) {
      return compareEqualDistanceTimes(left, right, direction);
    }
    const paceCompared = compareNullable(
      getComparablePace(left),
      getComparablePace(right),
      direction,
    );
    if (paceCompared !== 0) {
      return paceCompared;
    }
    const dateCompared = compareNullable(getRaceDateValue(left), getRaceDateValue(right), "desc");
    if (dateCompared !== 0) {
      return dateCompared;
    }
    return compareRunnerNumbers(left, right);
  };

export function HorseRaceResultsTable({
  classConditionName,
  currentDistance,
  currentKeibajoCode,
  currentRaceDate,
  currentTrackCode,
  defaultIncludeClass,
  raceTimeStats,
  results,
  runners,
  source,
  sourceScope,
}: HorseRaceResultsTableProps) {
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();
  const baseDistance = Number(cleanText(currentDistance, ""));
  const showLast3fColumn = !(source === "nar" && isBanEiKeibajoCode(currentKeibajoCode));
  const defaultNarFilterEnabled = source === "nar";
  const [distanceMin, setDistanceMin] = useState(
    Number.isFinite(baseDistance) && baseDistance > 0 ? String(baseDistance - 100) : "",
  );
  const [distanceMax, setDistanceMax] = useState(
    Number.isFinite(baseDistance) && baseDistance > 0 ? String(baseDistance + 200) : "",
  );
  const [limit, setLimit] = useState<ResultLimit>("1");
  const [finishRankLimit, setFinishRankLimit] = useState("5");
  const [finishRankLimitTouched, setFinishRankLimitTouched] = useState(false);
  const [includeOutOfRangeFallback, setIncludeOutOfRangeFallback] = useState(true);
  const [distanceMinTouched, setDistanceMinTouched] = useState(false);
  const [sameDistanceOnly, setSameDistanceOnly] = useState(false);
  const [sameKeibajoOnly, setSameKeibajoOnly] = useState(false);
  const [sameJockeyOnly, setSameJockeyOnly] = useState(defaultNarFilterEnabled);
  const [sameJockeyTouched, setSameJockeyTouched] = useState(false);
  const [expandedRunnerNumber, setExpandedRunnerNumber] = useState<string | null>(null);
  const [recentMonths, setRecentMonths] = useState(String(DEFAULT_RECENT_MONTHS));
  const [recentMonthsTouched, setRecentMonthsTouched] = useState(false);
  const [sort, setSort] = useState<{ direction: SortDirection; key: SortKey }>({
    direction: "asc",
    key: "sohaTime",
  });
  const [showChart, setShowChart] = useState(DEFAULT_SHOW_RESULTS_CHART);
  const hasEditedView = useRef(false);
  const sourceScopeChecked = sourceScope === source;
  const sourceScopeLabel = source === "jra" ? "中央競馬のみ" : "地方競馬のみ";
  const updateSourceScope = (checked: boolean) => {
    const nextParams = new URLSearchParams(searchParams.toString());
    if (checked) {
      nextParams.set("resultsSourceScope", source);
    } else {
      nextParams.delete("resultsSourceScope");
    }
    const query = nextParams.toString();
    router.replace(query ? `${pathname}?${query}` : pathname, { scroll: false });
  };
  const runnerNumberOptions = useMemo(
    () => getRunnerNumberOptions(runners, results),
    [results, runners],
  );
  const classFilterOptions = useMemo(
    () => getClassFilterOptions(classConditionName),
    [classConditionName],
  );
  const [classFilter, setClassFilter] = useState<string>(() =>
    defaultIncludeClass && classFilterOptions.length > 0
      ? (classFilterOptions.at(-1) ?? "all")
      : "all",
  );
  const [classFilterTouched, setClassFilterTouched] = useState(false);
  const [selectedRunnerNumbers, setSelectedRunnerNumbers] = useState<string[]>(() =>
    getRunnerNumberOptions(runners, results),
  );
  const selectedRunnerNumberSet = useMemo(
    () => new Set(selectedRunnerNumbers),
    [selectedRunnerNumbers],
  );
  const shouldDisableDefaultSameJockey = useMemo(() => {
    if (
      source !== "nar" ||
      sameJockeyTouched ||
      !sameJockeyOnly ||
      runnerNumberOptions.length === 0
    ) {
      return false;
    }

    const matchedRunnerNumbers = new Set(
      results
        .filter(
          (result) =>
            normalizeText(result.currentJockey) === normalizeText(result.kishumeiRyakusho),
        )
        .map((result) => cleanText(result.currentUmaban, ""))
        .filter(Boolean),
    );

    return runnerNumberOptions.some((runnerNumber) => !matchedRunnerNumbers.has(runnerNumber));
  }, [results, runnerNumberOptions, sameJockeyOnly, sameJockeyTouched, source]);

  useEffect(() => {
    if (shouldDisableDefaultSameJockey) {
      setSameJockeyOnly(false);
    }
  }, [shouldDisableDefaultSameJockey]);

  const debutRunners = useMemo(() => {
    const resultRunnerNumbers = new Set(
      results.map((result) => cleanText(result.currentUmaban, "")).filter(Boolean),
    );
    return runners.filter(
      (runner) =>
        selectedRunnerNumberSet.has(cleanText(runner.umaban, "")) &&
        !resultRunnerNumbers.has(cleanText(runner.umaban, "")),
    );
  }, [results, runners, selectedRunnerNumberSet]);

  const isCurrentKeibajo = useCallback(
    (keibajoCode: string | null | undefined): boolean =>
      cleanText(keibajoCode, "") === cleanText(currentKeibajoCode, ""),
    [currentKeibajoCode],
  );

  const isCurrentDistance = useCallback(
    (distance: string | null | undefined): boolean =>
      cleanText(distance, "") === cleanText(currentDistance, ""),
    [currentDistance],
  );

  const visibleResultsState = useMemo(() => {
    const min = Number(distanceMin);
    const max = Number(distanceMax);
    const hasMin = Number.isFinite(min);
    const hasMax = Number.isFinite(max);
    const limitCount = limit === "all" ? null : Number(limit);
    const parsedFinishRankLimit = Number(finishRankLimit);
    const hasFinishRankLimit =
      finishRankLimit.trim() !== "" &&
      Number.isInteger(parsedFinishRankLimit) &&
      parsedFinishRankLimit > 0;
    const recentMonthsValue = Number(recentMonths);
    const recentDateMin =
      recentMonths.trim() !== "" && Number.isFinite(recentMonthsValue) && recentMonthsValue > 0
        ? getDateMonthsBefore(currentRaceDate, recentMonthsValue)
        : null;
    const oldestResultDate = results.reduce<number | null>((oldest, result) => {
      const raceDate = getRaceDateValue(result);
      if (raceDate === null || !Number.isFinite(raceDate)) {
        return oldest;
      }
      return oldest === null ? raceDate : Math.min(oldest, raceDate);
    }, null);
    const oldestResultMonths =
      oldestResultDate === null
        ? recentMonthsValue
        : (getMonthsBetweenRaceDates(String(oldestResultDate), currentRaceDate) ??
          recentMonthsValue);
    const recentMonthsRelaxLimit = Math.max(
      recentMonthsValue,
      Math.ceil(oldestResultMonths / RECENT_MONTHS_RELAX_STEP) * RECENT_MONTHS_RELAX_STEP,
    );
    const resultRunnerNumbers = getCoveredRunnerNumbers(results);
    const requiredRunnerNumbers =
      runners.length > 0
        ? selectedRunnerNumbers
            .filter((runnerNumber) => resultRunnerNumbers.has(runnerNumber))
            .toSorted((left, right) => Number(left) - Number(right))
        : [];
    const coversRequiredRunners = (visibleRows: HorseRaceResult[]): boolean => {
      if (requiredRunnerNumbers.length === 0) {
        return true;
      }

      const visibleRunnerNumbers = getCoveredRunnerNumbers(visibleRows);
      return requiredRunnerNumbers.every((runnerNumber) => visibleRunnerNumbers.has(runnerNumber));
    };

    const distanceMinRelaxSteps =
      !sameDistanceOnly && hasMin && min > 0 ? Math.max(0, Math.floor(min / 100)) : 0;

    const isDistanceMatched = (result: HorseRaceResult, activeMin: number): boolean => {
      const distance = getDistanceValue(result);
      if (distance === null) {
        return false;
      }
      return (!hasMin || distance >= activeMin) && (!hasMax || distance <= max);
    };

    const compareByComparableRaceTimeAsc = createComparableRaceTimeComparator({
      baseDistance,
      currentKeibajoCode,
      currentTrackCode,
      direction: "asc",
    });
    const compareBySelectedSort =
      sort.key === "sohaTime"
        ? createComparableRaceTimeComparator({
            baseDistance,
            currentKeibajoCode,
            currentTrackCode,
            direction: sort.direction,
          })
        : (left: HorseRaceResult, right: HorseRaceResult): number => {
            const primary = compareNullable(
              getSortValue(left, sort.key),
              getSortValue(right, sort.key),
              sort.direction,
            );
            if (primary !== 0) {
              return primary;
            }
            return compareByComparableRaceTimeAsc(left, right);
          };

    const getVisibleResults = ({
      activeClassFilter,
      activeRecentDateMin,
      activeSameJockeyOnly,
      distanceRelaxSteps,
      useFinishRankFilter,
    }: {
      activeClassFilter: string;
      activeRecentDateMin: number | null;
      activeSameJockeyOnly: boolean;
      distanceRelaxSteps: number;
      useFinishRankFilter: boolean;
    }): HorseRaceResult[] => {
      const perHorseCount = new Map<string, number>();
      const groupedResults = new Map<string, HorseRaceResult[]>();
      const activeMin = hasMin ? Math.max(0, min - distanceRelaxSteps * 100) : min;

      for (const result of results) {
        const runnerNumber = cleanText(result.currentUmaban, "");
        if (runnerNumberOptions.length > 0 && !selectedRunnerNumberSet.has(runnerNumber)) {
          continue;
        }
        const distance = getDistanceValue(result);
        if (distance === null) {
          continue;
        }
        const finishRank = parseNumber(result.kakuteiChakujun);
        // Rows without a confirmed finish are always excluded from the main table
        // (the expandable detail table still shows them); relaxation never reintroduces them.
        if (finishRank === null) {
          continue;
        }
        if (useFinishRankFilter && hasFinishRankLimit && finishRank > parsedFinishRankLimit) {
          continue;
        }
        const jockeyMatched =
          !activeSameJockeyOnly ||
          normalizeText(result.currentJockey) === normalizeText(result.kishumeiRyakusho);
        if (!jockeyMatched) {
          continue;
        }
        if (sameKeibajoOnly && !isCurrentKeibajo(result.keibajoCode)) {
          continue;
        }
        if (sameDistanceOnly && !isCurrentDistance(result.kyori)) {
          continue;
        }
        if (activeClassFilter !== "all" && !isClassMatched(result, activeClassFilter)) {
          continue;
        }
        const raceDate = getRaceDateValue(result);
        if (activeRecentDateMin !== null && (raceDate === null || raceDate < activeRecentDateMin)) {
          continue;
        }
        const key = result.currentUmaban ?? "";
        groupedResults.set(key, [...(groupedResults.get(key) ?? []), result]);
      }

      const selectedResults = [...groupedResults.values()].flatMap((horseResults) => {
        const inRangeResults = horseResults.filter((result) =>
          isDistanceMatched(result, activeMin),
        );
        const shouldUseFallback =
          !sameDistanceOnly && inRangeResults.length === 0 && includeOutOfRangeFallback;
        // Comparison-oriented race-time order drives which races survive the per-horse limit.
        const prioritizedResults = shouldUseFallback
          ? horseResults.toSorted(compareByComparableRaceTimeAsc)
          : inRangeResults.toSorted(compareByComparableRaceTimeAsc);

        return prioritizedResults.filter((result) => {
          if (limitCount === null) {
            return true;
          }
          const key = result.currentUmaban ?? "";
          const current = perHorseCount.get(key) ?? 0;
          if (current >= limitCount) {
            return false;
          }
          perHorseCount.set(key, current + 1);
          return true;
        });
      });

      const distanceFilteredResults = selectedResults.filter((result) => {
        const distance = getDistanceValue(result);
        return distance !== null;
      });
      return distanceFilteredResults.toSorted(compareBySelectedSort);
    };

    const initialOptions = {
      activeClassFilter: classFilter,
      activeRecentDateMin: recentDateMin,
      activeSameJockeyOnly: sameJockeyOnly,
      distanceRelaxSteps: 0,
      useFinishRankFilter: true,
    };
    let currentOptions = initialOptions;
    let filteredResults = getVisibleResults(currentOptions);
    let relaxedDistanceMin: string | null = null;
    let relaxedClassFilter: string | null = null;
    let relaxedRecentMonths: string | null = null;
    let relaxedSameJockeyOnly: boolean | null = null;
    let shouldRelaxFinishRankLimit = false;

    const shouldUseCandidate = (candidate: HorseRaceResult[]): boolean => {
      const currentCovers = coversRequiredRunners(filteredResults);
      const candidateCovers = coversRequiredRunners(candidate);
      if (candidateCovers && !currentCovers) {
        return true;
      }
      if (candidate.length > filteredResults.length && !currentCovers) {
        return true;
      }
      return filteredResults.length === 0 && candidate.length > 0;
    };

    const applyCandidate = (nextOptions: typeof currentOptions, onApply: () => void): boolean => {
      if (coversRequiredRunners(filteredResults) && filteredResults.length > 0) {
        return true;
      }
      const candidate = getVisibleResults(nextOptions);
      if (!shouldUseCandidate(candidate) && candidate.length < filteredResults.length) {
        return false;
      }
      currentOptions = nextOptions;
      filteredResults = candidate;
      onApply();
      return coversRequiredRunners(filteredResults) && filteredResults.length > 0;
    };
    const needsRelaxation = (): boolean =>
      filteredResults.length === 0 || !coversRequiredRunners(filteredResults);

    if (!finishRankLimitTouched && hasFinishRankLimit && needsRelaxation()) {
      applyCandidate({ ...currentOptions, useFinishRankFilter: false }, () => {
        shouldRelaxFinishRankLimit = true;
      });
    }

    if (!distanceMinTouched && needsRelaxation() && distanceMinRelaxSteps > 0) {
      for (let relaxStep = 1; relaxStep <= distanceMinRelaxSteps; relaxStep += 1) {
        const applied = applyCandidate({ ...currentOptions, distanceRelaxSteps: relaxStep }, () => {
          relaxedDistanceMin = String(Math.max(0, min - relaxStep * 100));
        });
        if (applied || relaxedDistanceMin !== null) {
          break;
        }
      }
    }

    if (!classFilterTouched && classFilter !== "all" && needsRelaxation()) {
      applyCandidate({ ...currentOptions, activeClassFilter: "all" }, () => {
        relaxedClassFilter = "all";
      });
    }

    if (
      !recentMonthsTouched &&
      recentDateMin !== null &&
      Number.isFinite(recentMonthsValue) &&
      recentMonthsValue > 0 &&
      needsRelaxation()
    ) {
      for (
        let activeRecentMonths = recentMonthsValue + RECENT_MONTHS_RELAX_STEP;
        activeRecentMonths <= recentMonthsRelaxLimit;
        activeRecentMonths += RECENT_MONTHS_RELAX_STEP
      ) {
        const activeRecentDateMin = getDateMonthsBefore(currentRaceDate, activeRecentMonths);
        const applied = applyCandidate({ ...currentOptions, activeRecentDateMin }, () => {
          relaxedRecentMonths = String(activeRecentMonths);
        });
        if (applied || relaxedRecentMonths !== null) {
          break;
        }
      }
    }

    if (!sameJockeyTouched && sameJockeyOnly && needsRelaxation()) {
      applyCandidate({ ...currentOptions, activeSameJockeyOnly: false }, () => {
        relaxedSameJockeyOnly = false;
      });
    }

    return {
      relaxedClassFilter,
      relaxedDistanceMin,
      relaxedRecentMonths,
      relaxedSameJockeyOnly,
      shouldRelaxFinishRankLimit,
      results: filteredResults,
    };
  }, [
    baseDistance,
    classFilter,
    currentKeibajoCode,
    currentRaceDate,
    currentTrackCode,
    distanceMinTouched,
    distanceMax,
    distanceMin,
    classFilterTouched,
    finishRankLimit,
    finishRankLimitTouched,
    includeOutOfRangeFallback,
    isCurrentDistance,
    isCurrentKeibajo,
    limit,
    recentMonths,
    recentMonthsTouched,
    results,
    runnerNumberOptions.length,
    runners.length,
    sameDistanceOnly,
    sameKeibajoOnly,
    sameJockeyOnly,
    sameJockeyTouched,
    selectedRunnerNumbers,
    selectedRunnerNumberSet,
    sort,
  ]);
  const visibleResults = visibleResultsState.results;
  const showRacePacePrediction = isCornerPacePredictionSupported({
    distance: currentDistance,
    keibajoCode: currentKeibajoCode,
    source,
  });

  useEffect(() => {
    if (!showRacePacePrediction) {
      return;
    }
    window.dispatchEvent(
      new CustomEvent(RACE_PACE_PREDICTION_RESULTS_EVENT, {
        detail: {
          rows: buildRacePacePredictionRowsFromResults({
            currentConditionName: classConditionName,
            currentDistance,
            currentRaceDate,
            currentSource: source,
            currentTrackCode,
            results: visibleResults,
            runners,
          }),
        },
      }),
    );
  }, [
    classConditionName,
    currentDistance,
    currentRaceDate,
    currentTrackCode,
    runners,
    showRacePacePrediction,
    source,
    visibleResults,
  ]);

  useEffect(() => {
    if (visibleResultsState.shouldRelaxFinishRankLimit) {
      setFinishRankLimit("");
    }
  }, [visibleResultsState.shouldRelaxFinishRankLimit]);

  useEffect(() => {
    if (visibleResultsState.relaxedDistanceMin !== null) {
      setDistanceMin(visibleResultsState.relaxedDistanceMin);
    }
  }, [visibleResultsState.relaxedDistanceMin]);

  useEffect(() => {
    if (visibleResultsState.relaxedClassFilter !== null) {
      setClassFilter(visibleResultsState.relaxedClassFilter);
    }
  }, [visibleResultsState.relaxedClassFilter]);

  useEffect(() => {
    if (visibleResultsState.relaxedRecentMonths !== null) {
      setRecentMonths(visibleResultsState.relaxedRecentMonths);
    }
  }, [visibleResultsState.relaxedRecentMonths]);

  useEffect(() => {
    if (visibleResultsState.relaxedSameJockeyOnly !== null) {
      setSameJockeyOnly(visibleResultsState.relaxedSameJockeyOnly);
    }
  }, [visibleResultsState.relaxedSameJockeyOnly]);

  useEffect(() => {
    const loadState = { cancelled: false };
    void loadResultsChartForCurrentUser()
      .then((stored) => {
        if (loadState.cancelled || hasEditedView.current) {
          return undefined;
        }
        setShowChart(stored);
        return undefined;
      })
      .catch(() => undefined);
    return () => {
      loadState.cancelled = true;
    };
  }, []);

  const changeShowChart = (nextShowChart: boolean) => {
    hasEditedView.current = true;
    setShowChart(nextShowChart);
    void persistResultsChartForCurrentUser(nextShowChart).catch(() => undefined);
  };

  const raceResultsByRunnerNumber = useMemo(() => {
    const groupedResults = new Map<string, HorseRaceResult[]>();
    for (const result of results) {
      const runnerNumber = cleanText(result.currentUmaban, "");
      if (!runnerNumber) {
        continue;
      }
      groupedResults.set(runnerNumber, [...(groupedResults.get(runnerNumber) ?? []), result]);
    }
    return new Map(
      [...groupedResults.entries()].map(([runnerNumber, horseResults]) => [
        runnerNumber,
        horseResults.toSorted((left, right) => {
          const dateCompared = compareNullable(
            getRaceDateValue(left),
            getRaceDateValue(right),
            "desc",
          );
          if (dateCompared !== 0) {
            return dateCompared;
          }
          return Number(right.raceBango ?? 0) - Number(left.raceBango ?? 0);
        }),
      ]),
    );
  }, [results]);

  const toggleRunnerNumber = (runnerNumber: string) => {
    setSelectedRunnerNumbers((current) =>
      current.includes(runnerNumber)
        ? current.filter((number) => number !== runnerNumber)
        : [...current, runnerNumber].toSorted((left, right) => Number(left) - Number(right)),
    );
  };

  const changeSort = (key: SortKey) => {
    setSort((current) => ({
      direction: current.key === key && current.direction === "asc" ? "desc" : "asc",
      key,
    }));
  };

  const renderResultCells = (result: HorseRaceResult) => {
    const jockeyMatched =
      normalizeText(result.currentJockey) === normalizeText(result.kishumeiRyakusho);

    return (
      <>
        <td className={jockeyMatched ? "race-results-jockey-match-cell" : undefined}>
          {cleanText(result.currentJockey)}
        </td>
        <td>{formatSexAge(result.currentSeibetsuCode, result.currentBarei)}</td>
        <td>{formatDate(result.kaisaiNen, result.kaisaiTsukihi)}</td>
        <td
          className={isCurrentKeibajo(result.keibajoCode) ? "race-results-match-cell" : undefined}
        >
          {formatKeibajo(result.keibajoCode)}
        </td>
        <td className={isCurrentDistance(result.kyori) ? "race-results-match-cell" : undefined}>
          {formatDistance(result.kyori)}
        </td>
        <td>{formatRank(result.kakuteiChakujun)}</td>
        <td>{formatCornerRanks(result)}</td>
        <td>{formatTenthsTime(result.sohaTime, isBanEiKeibajoCode(result.keibajoCode))}</td>
        {showLast3fColumn ? <td>{formatDecimalTenths(result.kohan3f)}</td> : null}
        <td className={jockeyMatched ? "race-results-jockey-match-cell" : undefined}>
          {cleanText(result.kishumeiRyakusho)}
        </td>
        <td>{formatSexAge(result.seibetsuCode, result.barei)}</td>
        <td>{formatCarriedWeight(result.futanJuryo, isBanEiKeibajoCode(result.keibajoCode))}</td>
        <td>
          {formatHorseWeight(
            result.bataiju,
            result.zogenFugo,
            result.zogenSa,
            isBanEiKeibajoCode(result.keibajoCode),
          )}
        </td>
        <td>{formatOdds(result.tanshoOdds)}</td>
        <td>{formatRunnerValue(result.tanshoNinkijun, "00")}</td>
        <td>{formatTimeDifference(result.timeSa)}</td>
        <td>{formatRaceConditions(result)}</td>
        <td className="race-results-name-cell">{formatRaceName(result)}</td>
        <td>{formatRaceNumber(result.raceBango)}</td>
        <td>{formatTrack(result.trackCode)}</td>
        <td>{formatWeather(result.tenkoCode)}</td>
        <td>
          <FrameNumberBadge value={result.wakuban} />
        </td>
        <td>{formatRunnerNumber(result.umaban)}</td>
      </>
    );
  };

  const renderSortButton = (key: SortKey) => {
    const isCurrent = sort.key === key;
    const direction = isCurrent ? sort.direction : "asc";
    const nextDirection = isCurrent && sort.direction === "asc" ? "desc" : "asc";

    return (
      <button
        aria-label={`${SORT_LABELS[key]}を${nextDirection === "asc" ? "昇順" : "降順"}で並び替え`}
        className="race-results-sort-button"
        type="button"
        onClick={() => {
          changeSort(key);
        }}
      >
        <span>{SORT_LABELS[key]}</span>
        <small>{direction === "asc" ? "昇順" : "降順"}</small>
      </button>
    );
  };

  if (results.length === 0 && debutRunners.length === 0) {
    return <p className="empty-state">出走予定馬の過去成績は見つかりませんでした。</p>;
  }

  return (
    <>
      <MobileFilterDisclosure title="条件設定">
        <section className="race-results-filter-panel" aria-label="race result filters">
          <label>
            <span>距離 下限</span>
            <input
              inputMode="numeric"
              type="number"
              value={distanceMin}
              onChange={(event) => {
                setDistanceMinTouched(true);
                setDistanceMin(event.currentTarget.value);
              }}
            />
          </label>
          <label>
            <span>距離 上限</span>
            <input
              inputMode="numeric"
              type="number"
              value={distanceMax}
              onChange={(event) => {
                setDistanceMax(event.currentTarget.value);
              }}
            />
          </label>
          <label>
            <span>馬ごとの表示数</span>
            <select
              value={limit}
              onChange={(event) => {
                setLimit(toResultLimit(event.currentTarget.value));
              }}
            >
              <option value="all">全件</option>
              <option value="1">1件</option>
              <option value="3">3件</option>
              <option value="5">5件</option>
              <option value="10">10件</option>
            </select>
          </label>
          <label>
            <span>着順で絞り込む（◯着以内）</span>
            <input
              inputMode="numeric"
              min="1"
              placeholder="制限なし"
              type="number"
              value={finishRankLimit}
              onChange={(event) => {
                setFinishRankLimitTouched(true);
                setFinishRankLimit(event.currentTarget.value);
              }}
            />
          </label>
          {classFilterOptions.length > 0 ? (
            <label>
              <span>条件</span>
              <select
                value={classFilter}
                onChange={(event) => {
                  setClassFilterTouched(true);
                  setClassFilter(event.currentTarget.value);
                }}
              >
                <option value="all">全条件</option>
                {classFilterOptions.map((option) => (
                  <option key={option} value={option}>
                    {option}
                  </option>
                ))}
              </select>
            </label>
          ) : null}
          <label className="race-results-checkbox-label">
            <span>{sourceScopeLabel}</span>
            <span className="race-results-checkbox-control">
              <input
                aria-label={sourceScopeLabel}
                checked={sourceScopeChecked}
                type="checkbox"
                onChange={(event) => {
                  updateSourceScope(event.currentTarget.checked);
                }}
              />
            </span>
          </label>
          <label className="race-results-checkbox-label">
            <span>出走予定と同じ騎手</span>
            <span className="race-results-checkbox-control">
              <input
                aria-label="出走予定と同じ騎手"
                checked={sameJockeyOnly}
                type="checkbox"
                onChange={(event) => {
                  setSameJockeyTouched(true);
                  setSameJockeyOnly(event.currentTarget.checked);
                }}
              />
            </span>
          </label>
          <label className="race-results-checkbox-label">
            <span>出走予定と同じ競馬場</span>
            <span className="race-results-checkbox-control">
              <input
                aria-label="出走予定と同じ競馬場"
                checked={sameKeibajoOnly}
                type="checkbox"
                onChange={(event) => {
                  setSameKeibajoOnly(event.currentTarget.checked);
                }}
              />
            </span>
          </label>
          <label className="race-results-checkbox-label">
            <span>同距離のみ</span>
            <span className="race-results-checkbox-control">
              <input
                aria-label="出走予定と同じ距離のみ"
                checked={sameDistanceOnly}
                type="checkbox"
                onChange={(event) => {
                  setSameDistanceOnly(event.currentTarget.checked);
                }}
              />
            </span>
          </label>
          <label>
            <span>表示期間（直近◯ヶ月）</span>
            <input
              inputMode="numeric"
              min="1"
              placeholder={String(DEFAULT_RECENT_MONTHS)}
              step={RECENT_MONTHS_STEP}
              type="number"
              value={recentMonths}
              onChange={(event) => {
                setRecentMonthsTouched(true);
                setRecentMonths(event.currentTarget.value);
              }}
            />
          </label>
          <label className="race-results-checkbox-label">
            <span>近い距離も表示</span>
            <span className="race-results-checkbox-control">
              <input
                aria-label="近い距離も表示"
                checked={includeOutOfRangeFallback}
                disabled={sameDistanceOnly}
                type="checkbox"
                onChange={(event) => {
                  setIncludeOutOfRangeFallback(event.currentTarget.checked);
                }}
              />
            </span>
          </label>
          {runnerNumberOptions.length > 0 ? (
            <fieldset className="race-results-runner-filter">
              <legend>馬番号</legend>
              <div className="race-results-runner-filter-actions">
                <button
                  type="button"
                  onClick={() => {
                    setSelectedRunnerNumbers(runnerNumberOptions);
                  }}
                >
                  全てチェック
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setSelectedRunnerNumbers([]);
                  }}
                >
                  全て外す
                </button>
              </div>
              <div>
                {runnerNumberOptions.map((runnerNumber) => (
                  <label key={runnerNumber}>
                    <input
                      checked={selectedRunnerNumberSet.has(runnerNumber)}
                      type="checkbox"
                      onChange={() => {
                        toggleRunnerNumber(runnerNumber);
                      }}
                    />
                    <span>{formatRunnerNumber(runnerNumber)}</span>
                  </label>
                ))}
              </div>
            </fieldset>
          ) : null}
          <span className="race-results-filter-count">
            {visibleResults.length} / {results.length} 件
          </span>
        </section>
      </MobileFilterDisclosure>
      {raceTimeStats ? <RaceTimeStatsMetrics stats={raceTimeStats} /> : null}
      {debutRunners.length > 0 ? (
        <section className="race-results-newcomer-panel" aria-label="newcomer runners">
          <h3>新馬</h3>
          <div>
            {debutRunners.map((runner) => (
              <span key={runner.umaban ?? runner.kettoTorokuBango ?? runner.bamei}>
                <strong>{formatRunnerNumber(runner.umaban)}</strong>
                {getRunnerDisplayNames(runner).horse}
              </span>
            ))}
          </div>
        </section>
      ) : null}
      <ResultsViewToggle showChart={showChart} onShowChart={changeShowChart} />
      {visibleResults.length === 0 ? (
        <p className="empty-state">条件に一致する競走成績はありません。</p>
      ) : null}
      {visibleResults.length > 0 && showChart ? (
        <HorseRaceTimeChart
          currentDistance={currentDistance}
          keibajoCode={currentKeibajoCode}
          results={visibleResults}
          stats={raceTimeStats}
        />
      ) : null}
      {visibleResults.length > 0 && !showChart ? (
        <div className="race-results-table-wrap">
          <table className="race-results-table">
            <colgroup>
              <col className="race-results-col-runner-number" />
              <col className="race-results-col-horse-name" />
              <col className="race-results-col-person" />
              <col className="race-results-col-sex-age" />
              <col className="race-results-col-date" />
              <col className="race-results-col-keibajo" />
              <col className="race-results-col-distance" />
              <col className="race-results-col-rank" />
              <col className="race-results-col-dynamic" />
              {showLast3fColumn ? <col className="race-results-col-sort" /> : null}
              <col className="race-results-col-sort" />
              <col className="race-results-col-person" />
              <col className="race-results-col-sex-age" />
              <col className="race-results-col-weight-carried" />
              <col className="race-results-col-horse-weight" />
              <col className="race-results-col-odds" />
              <col className="race-results-col-rank" />
              <col className="race-results-col-margin" />
              <col className="race-results-col-dynamic" />
              <col className="race-results-col-dynamic-wide" />
              <col className="race-results-col-race-number" />
              <col className="race-results-col-track" />
              <col className="race-results-col-weather" />
              <col className="race-results-col-frame" />
              <col className="race-results-col-past-runner-number" />
            </colgroup>
            <thead>
              <tr>
                <th>馬番号</th>
                <th>馬名</th>
                <th>予定騎手</th>
                <th>現在性齢</th>
                <th>{renderSortButton("date")}</th>
                <th>競馬場</th>
                <th>距離</th>
                <th>着順</th>
                <th>コーナー順位</th>
                <th>{renderSortButton("sohaTime")}</th>
                {showLast3fColumn ? <th>{renderSortButton("kohan3f")}</th> : null}
                <th>過去騎手</th>
                <th>過去性齢</th>
                <th>負担</th>
                <th>馬体重</th>
                <th>単勝</th>
                <th>人気</th>
                <th>着差</th>
                <th>条件</th>
                <th>レース名</th>
                <th>R</th>
                <th>コース</th>
                <th>天候</th>
                <th>過去枠</th>
                <th>過去馬番</th>
              </tr>
            </thead>
            <tbody>
              {visibleResults.map((result) => {
                const runnerNumber = cleanText(result.currentUmaban, "");
                const expanded = expandedRunnerNumber === runnerNumber;
                const detailResults = raceResultsByRunnerNumber.get(runnerNumber) ?? [];

                return (
                  <Fragment
                    key={[
                      "result-group",
                      result.currentUmaban,
                      result.kaisaiNen,
                      result.kaisaiTsukihi,
                      result.keibajoCode,
                      result.raceBango,
                      result.kettoTorokuBango,
                    ].join("-")}
                  >
                    <tr
                      key={[
                        result.currentUmaban,
                        result.kaisaiNen,
                        result.kaisaiTsukihi,
                        result.keibajoCode,
                        result.raceBango,
                        result.kettoTorokuBango,
                      ].join("-")}
                    >
                      <td>{formatRunnerNumber(result.currentUmaban)}</td>
                      <td className="race-results-horse-cell">
                        <span>{cleanText(result.bamei)}</span>
                        <button
                          className="race-results-detail-button"
                          type="button"
                          onClick={() => {
                            setExpandedRunnerNumber((current) =>
                              current === runnerNumber ? null : runnerNumber,
                            );
                          }}
                        >
                          詳細
                        </button>
                      </td>
                      {renderResultCells(result)}
                    </tr>
                    {expanded ? (
                      <tr className="race-results-detail-row" key={`detail-${runnerNumber}`}>
                        <td aria-label="競走成績の詳細" colSpan={showLast3fColumn ? 25 : 24}>
                          <div className="race-results-detail-panel">
                            <table className="race-results-detail-table">
                              <thead>
                                <tr>
                                  <th>日付</th>
                                  <th>競馬場</th>
                                  <th>R</th>
                                  <th>距離</th>
                                  <th>着順</th>
                                  <th>コーナー順位</th>
                                  <th>レースタイム</th>
                                  {showLast3fColumn ? <th>上がり3F</th> : null}
                                  <th>過去騎手</th>
                                  <th>単勝</th>
                                  <th>人気</th>
                                  <th>レース名</th>
                                </tr>
                              </thead>
                              <tbody>
                                {detailResults.map((detail) => (
                                  <tr
                                    key={[
                                      "detail",
                                      detail.currentUmaban,
                                      detail.kaisaiNen,
                                      detail.kaisaiTsukihi,
                                      detail.keibajoCode,
                                      detail.raceBango,
                                      detail.umaban,
                                    ].join("-")}
                                  >
                                    <td>{formatDate(detail.kaisaiNen, detail.kaisaiTsukihi)}</td>
                                    <td>{formatKeibajo(detail.keibajoCode)}</td>
                                    <td>{formatRaceNumber(detail.raceBango)}</td>
                                    <td>{formatDistance(detail.kyori)}</td>
                                    <td>{formatRank(detail.kakuteiChakujun)}</td>
                                    <td>{formatCornerRanks(detail)}</td>
                                    <td>
                                      {formatTenthsTime(
                                        detail.sohaTime,
                                        isBanEiKeibajoCode(detail.keibajoCode),
                                      )}
                                    </td>
                                    {showLast3fColumn ? (
                                      <td>{formatDecimalTenths(detail.kohan3f)}</td>
                                    ) : null}
                                    <td>{cleanText(detail.kishumeiRyakusho)}</td>
                                    <td>{formatOdds(detail.tanshoOdds)}</td>
                                    <td>{formatRunnerValue(detail.tanshoNinkijun, "00")}</td>
                                    <td className="race-results-name-cell">
                                      {formatRaceName(detail)}
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </td>
                      </tr>
                    ) : null}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      ) : null}
    </>
  );
}
