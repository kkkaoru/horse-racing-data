"use client";

import { useEffect, useMemo, useState } from "react";

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
import type { HorseRaceResult, Runner } from "../../../lib/race-types";
import {
  formatCarriedWeight,
  formatHorseWeight,
  formatRunnerNumber,
  formatRunnerValue,
  formatSexAge,
} from "../../../lib/runner-format";

type ResultLimit = "all" | "1" | "3" | "5" | "10";
type SortDirection = "asc" | "desc";
type SortKey = "date" | "kohan3f" | "sohaTime";

interface HorseRaceResultsTableProps {
  currentDistance: string | null | undefined;
  currentKeibajoCode: string;
  currentRaceDate: string;
  results: HorseRaceResult[];
  runners: Runner[];
  source: "jra" | "nar";
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

const formatTenthsTime = (value: string | null | undefined): string => {
  const tenths = parseNumber(value);
  if (tenths === null) {
    return "-";
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

const normalizeText = (value: string | null | undefined): string =>
  cleanText(value, "").replace(/\s+/g, "").replace(/　+/g, "");

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
  currentDistance,
  currentKeibajoCode,
  currentRaceDate,
  results,
  runners,
  source,
}: HorseRaceResultsTableProps) {
  const baseDistance = Number(cleanText(currentDistance, ""));
  const defaultNarFilterEnabled = source === "nar";
  const [distanceMin, setDistanceMin] = useState(
    Number.isFinite(baseDistance) && baseDistance > 0 ? String(baseDistance - 100) : "",
  );
  const [distanceMax, setDistanceMax] = useState(
    Number.isFinite(baseDistance) && baseDistance > 0 ? String(baseDistance + 200) : "",
  );
  const [limit, setLimit] = useState<ResultLimit>("5");
  const [includeOutOfRangeFallback, setIncludeOutOfRangeFallback] = useState(true);
  const [sameJockeyOnly, setSameJockeyOnly] = useState(defaultNarFilterEnabled);
  const [sameJockeyTouched, setSameJockeyTouched] = useState(false);
  const [recentMonths, setRecentMonths] = useState(defaultNarFilterEnabled ? "18" : "");
  const [sort, setSort] = useState<{ direction: SortDirection; key: SortKey }>({
    direction: "asc",
    key: "sohaTime",
  });
  const runnerNumberOptions = useMemo(
    () => getRunnerNumberOptions(runners, results),
    [results, runners],
  );
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

  const visibleResults = useMemo(() => {
    const min = Number(distanceMin);
    const max = Number(distanceMax);
    const hasMin = Number.isFinite(min);
    const hasMax = Number.isFinite(max);
    const limitCount = limit === "all" ? null : Number(limit);
    const perHorseCount = new Map<string, number>();
    const groupedResults = new Map<string, HorseRaceResult[]>();
    const recentMonthsValue = Number(recentMonths);
    const recentDateMin =
      recentMonths.trim() !== "" && Number.isFinite(recentMonthsValue) && recentMonthsValue > 0
        ? getDateMonthsBefore(currentRaceDate, recentMonthsValue)
        : null;

    const isDistanceMatched = (result: HorseRaceResult): boolean => {
      const distance = getDistanceValue(result);
      if (distance === null) {
        return false;
      }
      return (!hasMin || distance >= min) && (!hasMax || distance <= max);
    };

    for (const result of results) {
      const runnerNumber = cleanText(result.currentUmaban, "");
      if (runnerNumberOptions.length > 0 && !selectedRunnerNumberSet.has(runnerNumber)) {
        continue;
      }
      const distance = getDistanceValue(result);
      if (distance === null) {
        continue;
      }
      const jockeyMatched =
        !sameJockeyOnly ||
        normalizeText(result.currentJockey) === normalizeText(result.kishumeiRyakusho);
      if (!jockeyMatched) {
        continue;
      }
      const raceDate = getRaceDateValue(result);
      if (recentDateMin !== null && (raceDate === null || raceDate < recentDateMin)) {
        continue;
      }
      const key = result.currentUmaban ?? "";
      groupedResults.set(key, [...(groupedResults.get(key) ?? []), result]);
    }

    const selectedResults = [...groupedResults.values()].flatMap((horseResults) => {
      const inRangeResults = horseResults.filter(isDistanceMatched);
      const shouldUseFallback = inRangeResults.length === 0 && includeOutOfRangeFallback;
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
  }, [
    baseDistance,
    currentRaceDate,
    distanceMax,
    distanceMin,
    includeOutOfRangeFallback,
    limit,
    recentMonths,
    results,
    runnerNumberOptions.length,
    sameJockeyOnly,
    selectedRunnerNumberSet,
    sort,
  ]);

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

  const isCurrentKeibajo = (keibajoCode: string | null | undefined): boolean =>
    cleanText(keibajoCode, "") === cleanText(currentKeibajoCode, "");

  const isCurrentDistance = (distance: string | null | undefined): boolean =>
    cleanText(distance, "") === cleanText(currentDistance, "");

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
      <section className="race-results-filter-panel" aria-label="race result filters">
        <label>
          <span>距離 下限</span>
          <input
            inputMode="numeric"
            type="number"
            value={distanceMin}
            onChange={(event) => {
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
        <label>
          <span>出走日からnヶ月以内</span>
          <input
            inputMode="numeric"
            min="1"
            placeholder="制限なし"
            type="number"
            value={recentMonths}
            onChange={(event) => {
              setRecentMonths(event.currentTarget.value);
            }}
          />
        </label>
        <label className="race-results-checkbox-label">
          <span>距離範囲外の近い成績も補完</span>
          <span className="race-results-checkbox-control">
            <input
              aria-label="距離範囲外の近い成績も補完"
              checked={includeOutOfRangeFallback}
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
              <col className="race-results-col-dynamic" />
              <col className="race-results-col-person" />
              <col className="race-results-col-sex-age" />
              <col className="race-results-col-date" />
              <col className="race-results-col-keibajo" />
              <col className="race-results-col-distance" />
              <col className="race-results-col-rank" />
              <col className="race-results-col-sort" />
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
                <th>{renderSortButton("sohaTime")}</th>
                <th>{renderSortButton("kohan3f")}</th>
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
                const jockeyMatched =
                  normalizeText(result.currentJockey) === normalizeText(result.kishumeiRyakusho);

                return (
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
                    <td className="race-results-horse-cell">{cleanText(result.bamei)}</td>
                    <td className={jockeyMatched ? "race-results-jockey-match-cell" : undefined}>
                      {cleanText(result.currentJockey)}
                    </td>
                    <td>{formatSexAge(result.currentSeibetsuCode, result.currentBarei)}</td>
                    <td>{formatDate(result.kaisaiNen, result.kaisaiTsukihi)}</td>
                    <td
                      className={
                        isCurrentKeibajo(result.keibajoCode) ? "race-results-match-cell" : undefined
                      }
                    >
                      {formatKeibajo(result.keibajoCode)}
                    </td>
                    <td
                      className={
                        isCurrentDistance(result.kyori) ? "race-results-match-cell" : undefined
                      }
                    >
                      {formatDistance(result.kyori)}
                    </td>
                    <td>{formatRank(result.kakuteiChakujun)}</td>
                    <td>{formatTenthsTime(result.sohaTime)}</td>
                    <td>{formatDecimalTenths(result.kohan3f)}</td>
                    <td className={jockeyMatched ? "race-results-jockey-match-cell" : undefined}>
                      {cleanText(result.kishumeiRyakusho)}
                    </td>
                    <td>{formatSexAge(result.seibetsuCode, result.barei)}</td>
                    <td>{formatCarriedWeight(result.futanJuryo, result.keibajoCode === "83")}</td>
                    <td>
                      {formatHorseWeight(
                        result.bataiju,
                        result.zogenFugo,
                        result.zogenSa,
                        result.keibajoCode === "83",
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
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}
