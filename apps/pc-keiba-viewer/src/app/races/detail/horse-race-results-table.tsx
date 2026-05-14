"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { Fragment, useCallback, useEffect, useMemo, useState } from "react";

import type { RaceSource } from "../../../lib/codes";
import {
  cleanText,
  formatDate,
  formatDistance,
  formatKeibajo,
  formatRaceNumber,
  formatTrack,
  formatWeather,
} from "../../../lib/format";
import { getRaceTags } from "../../../lib/race-classification";
import {
  buildRacePacePredictionRowsFromResults,
  isCornerPacePredictionSupported,
  RACE_PACE_PREDICTION_RESULTS_EVENT,
} from "../../../lib/race-pace-prediction";
import type { HorseRaceResult, Runner } from "../../../lib/race-types";
import {
  formatCarriedWeight,
  formatHorseWeight,
  formatRunnerNumber,
  formatRunnerValue,
  formatSexAge,
  isBanEiKeibajoCode,
} from "../../../lib/runner-format";
import { MobileFilterDisclosure } from "./mobile-filter-disclosure";

type ResultLimit = "all" | "1" | "3" | "5" | "10";
type SortDirection = "asc" | "desc";
type SortKey = "date" | "kohan3f" | "sohaTime";

interface HorseRaceResultsTableProps {
  classConditionName: string | null;
  currentDistance: string | null | undefined;
  currentKeibajoCode: string;
  currentRaceDate: string;
  currentTrackCode: string | null;
  defaultIncludeClass: boolean;
  results: HorseRaceResult[];
  runners: Runner[];
  source: RaceSource;
  sourceScope: RaceSource | "all";
}

const SORT_LABELS: Record<SortKey, string> = {
  date: "日付",
  kohan3f: "上がり3F",
  sohaTime: "レースタイム",
};

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

const getSortValue = (result: HorseRaceResult, key: SortKey): number | null => {
  if (key === "date") {
    return Number(`${result.kaisaiNen}${result.kaisaiTsukihi}`);
  }
  if (key === "kohan3f") {
    return parseNumber(result.kohan3f);
  }
  return parseNumber(result.sohaTime);
};

const getRaceDateValue = (result: HorseRaceResult): number | null =>
  Number(`${result.kaisaiNen}${result.kaisaiTsukihi}`);

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

const compareByTimeAndDate = (left: HorseRaceResult, right: HorseRaceResult): number => {
  const timeCompared = compareNullable(
    getSortValue(left, "sohaTime"),
    getSortValue(right, "sohaTime"),
    "asc",
  );
  if (timeCompared !== 0) {
    return timeCompared;
  }
  return compareNullable(getRaceDateValue(left), getRaceDateValue(right), "desc");
};

export function HorseRaceResultsTable({
  classConditionName,
  currentDistance,
  currentKeibajoCode,
  currentRaceDate,
  currentTrackCode,
  defaultIncludeClass,
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
  const [recentMonths, setRecentMonths] = useState(defaultNarFilterEnabled ? "18" : "");
  const [recentMonthsTouched, setRecentMonthsTouched] = useState(false);
  const [sort, setSort] = useState<{ direction: SortDirection; key: SortKey }>({
    direction: "asc",
    key: "sohaTime",
  });
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
        if (
          useFinishRankFilter &&
          hasFinishRankLimit &&
          (finishRank === null || finishRank > parsedFinishRankLimit)
        ) {
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
        const prioritizedResults = shouldUseFallback
          ? horseResults.toSorted((left, right) => {
              const leftDistance = getDistanceValue(left);
              const rightDistance = getDistanceValue(right);
              const leftDiff =
                leftDistance === null || !Number.isFinite(baseDistance)
                  ? null
                  : Math.abs(leftDistance - baseDistance);
              const rightDiff =
                rightDistance === null || !Number.isFinite(baseDistance)
                  ? null
                  : Math.abs(rightDistance - baseDistance);
              const distanceCompared = compareNullable(leftDiff, rightDiff, "asc");
              return distanceCompared !== 0 ? distanceCompared : compareByTimeAndDate(left, right);
            })
          : inRangeResults.toSorted(compareByTimeAndDate);

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

      return selectedResults
        .filter((result) => {
          const distance = getDistanceValue(result);
          return distance !== null;
        })
        .toSorted((left, right) => {
          const primary = compareNullable(
            getSortValue(left, sort.key),
            getSortValue(right, sort.key),
            sort.direction,
          );
          if (primary !== 0) {
            return primary;
          }
          if (sort.key !== "sohaTime") {
            const timeCompared = compareNullable(
              getSortValue(left, "sohaTime"),
              getSortValue(right, "sohaTime"),
              "asc",
            );
            if (timeCompared !== 0) {
              return timeCompared;
            }
          }
          const dateCompared = compareNullable(
            getRaceDateValue(left),
            getRaceDateValue(right),
            "desc",
          );
          if (dateCompared !== 0) {
            return dateCompared;
          }
          return Number(left.currentUmaban ?? 0) - Number(right.currentUmaban ?? 0);
        });
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

    if (!recentMonthsTouched && recentDateMin !== null && needsRelaxation()) {
      applyCandidate({ ...currentOptions, activeRecentDateMin: null }, () => {
        relaxedRecentMonths = "";
      });
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
    currentRaceDate,
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
        <td>{cleanText(result.wakuban)}</td>
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
            <span>着順 n着以内</span>
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
            <span>直近nヶ月</span>
            <input
              inputMode="numeric"
              min="1"
              placeholder="制限なし"
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
      {debutRunners.length > 0 ? (
        <section className="race-results-newcomer-panel" aria-label="newcomer runners">
          <h3>新馬</h3>
          <div>
            {debutRunners.map((runner) => (
              <span key={runner.umaban ?? runner.kettoTorokuBango ?? runner.bamei}>
                <strong>{formatRunnerNumber(runner.umaban)}</strong>
                {cleanText(runner.bamei)}
              </span>
            ))}
          </div>
        </section>
      ) : null}
      {visibleResults.length === 0 ? (
        <p className="empty-state">条件に一致する競走成績はありません。</p>
      ) : (
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
                        <td colSpan={showLast3fColumn ? 25 : 24}>
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
      )}
    </>
  );
}
