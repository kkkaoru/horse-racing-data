"use client";

import { useRouter, useSearchParams } from "next/navigation";
import type { ReactNode } from "react";
import { Fragment, useMemo, useState } from "react";

import { cleanText, formatDate, formatKeibajo, formatRaceNumber } from "../../../lib/format";
import type { BloodlineStatsRow, Runner, SimilarRaceStatsSettings } from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";
import { MobileFilterDisclosure } from "./mobile-filter-disclosure";

type BloodlineCategory = BloodlineStatsRow["category"];
type RateSortKey = "showRate" | "quinellaRate" | "winRate";
type SortDirection = "asc" | "desc";

type BloodlineScoreRow = {
  categoryRows: Partial<Record<BloodlineCategory, BloodlineStatsRow>>;
  categoryScores: Record<BloodlineCategory, number>;
  horseCount: number;
  horseName: string;
  horseNumber: string;
  rawScore: number;
  score: number;
  starts: number;
};

interface BloodlineStatsTableProps {
  conditionLabels: {
    age: string | null;
    class: string | null;
    distance: string | null;
    frame: string;
    monthWindow: string;
    raceNumber: string;
    raceSubtitle: string | null;
    raceTitle: string | null;
    sex: string | null;
    surface: string | null;
    turn: string | null;
    venue: string | null;
    weight: string | null;
  };
  rows: BloodlineStatsRow[];
  runners: Runner[];
  settings: SimilarRaceStatsSettings;
}

const SORT_LABELS: Record<RateSortKey, string> = {
  quinellaRate: "連対率",
  showRate: "複勝率",
  winRate: "勝率",
};

const CATEGORY_LABELS: Record<BloodlineCategory, string> = {
  damSire: "母父",
  sire: "父",
  sireSire: "父父",
};

const CATEGORY_TITLE_LABELS: Record<BloodlineCategory, string> = {
  damSire: "母父の勝率",
  sire: "父の勝率",
  sireSire: "父父の勝率",
};

const CATEGORY_ORDER: BloodlineCategory[] = ["sire", "damSire", "sireSire"];

const CATEGORY_SCORE_WEIGHTS: Record<BloodlineCategory, number> = {
  damSire: 0.35,
  sire: 0.45,
  sireSire: 0.2,
};

const METRIC_SCORE_WEIGHTS = {
  horseCount: 0.05,
  quinellaRate: 0.25,
  showRate: 0.35,
  starts: 0.1,
  winRate: 0.25,
};

type ToggleSettingKey = keyof Omit<
  SimilarRaceStatsSettings,
  "classConditionName" | "includeRunnerCount" | "runnerCount" | "years"
>;

const SETTING_PARAMS: Record<ToggleSettingKey, string> = {
  includeAge: "statsAge",
  includeClass: "statsClass",
  includeDistance: "statsDistance",
  includeFrame: "statsFrame",
  includeMonthWindow: "statsMonthWindow",
  includeRaceNumber: "statsRaceNumber",
  includeRaceSubtitle: "statsRaceSubtitle",
  includeRaceTitle: "statsRaceTitle",
  includeSex: "statsSex",
  includeSurface: "statsSurface",
  includeTurn: "statsTurn",
  includeVenue: "statsVenue",
  includeWeight: "statsWeight",
};

const formatRate = (value: number): string => `${value.toFixed(1)}%`;

const formatScore = (value: number): string => value.toFixed(2);

const formatDetailDate = (date: string): string =>
  date.length === 8 ? formatDate(date.slice(0, 4), date.slice(4, 8)) : "-";

const parseNumber = (value: string): number | null => {
  const cleaned = value.trim();
  if (!cleaned || /^0+$/.test(cleaned)) {
    return null;
  }
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};

const formatTenthsTime = (value: string): string => {
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

const formatOdds = (value: string): string => {
  const odds = parseNumber(value);
  return odds === null ? "-" : (odds / 10).toFixed(1);
};

const formatRank = (value: string): string => {
  const rank = parseNumber(value);
  return rank === null ? "-" : String(rank);
};

const splitHorseNumbers = (value: string): string[] =>
  value
    .split(",")
    .map((horseNumber) => cleanText(horseNumber, ""))
    .filter(Boolean);

const normalize = (value: number, max: number): number => (max > 0 ? value / max : 0);

const isTargetBloodline = (value: string | undefined, targetName: string): boolean =>
  cleanText(value, "") === cleanText(targetName, "");

export function BloodlineStatsTable({
  conditionLabels,
  rows,
  runners,
  settings,
}: BloodlineStatsTableProps) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [sortKey, setSortKey] = useState<RateSortKey>("showRate");
  const [sortDirection, setSortDirection] = useState<SortDirection>("desc");
  const [expandedRowKey, setExpandedRowKey] = useState<string | null>(null);
  const [expandedScoreRowKey, setExpandedScoreRowKey] = useState<string | null>(null);

  const groupedRows = useMemo(() => {
    const sortedRows = rows.toSorted((left, right) => {
      const compared =
        sortDirection === "asc" ? left[sortKey] - right[sortKey] : right[sortKey] - left[sortKey];
      return compared === 0 ? right.starts - left.starts : compared;
    });

    return CATEGORY_ORDER.map((category) => ({
      category,
      rows: sortedRows.filter((row) => row.category === category),
    }));
  }, [rows, sortDirection, sortKey]);

  const scoreRows = useMemo(() => {
    const maxStarts = Math.max(...rows.map((row) => row.starts), 0);
    const maxHorseCount = Math.max(...rows.map((row) => row.horseCount), 0);
    const rowScores = new Map<BloodlineStatsRow, number>();

    for (const row of rows) {
      rowScores.set(
        row,
        (row.showRate / 100) * METRIC_SCORE_WEIGHTS.showRate +
          (row.quinellaRate / 100) * METRIC_SCORE_WEIGHTS.quinellaRate +
          (row.winRate / 100) * METRIC_SCORE_WEIGHTS.winRate +
          normalize(row.starts, maxStarts) * METRIC_SCORE_WEIGHTS.starts +
          normalize(row.horseCount, maxHorseCount) * METRIC_SCORE_WEIGHTS.horseCount,
      );
    }

    const categoryRowsByHorse = new Map<
      string,
      Partial<Record<BloodlineCategory, BloodlineStatsRow>>
    >();
    for (const row of rows) {
      for (const horseNumber of splitHorseNumbers(row.currentHorseNumbers)) {
        const currentRows = categoryRowsByHorse.get(horseNumber);
        categoryRowsByHorse.set(horseNumber, { ...currentRows, [row.category]: row });
      }
    }

    const rawRows = runners.map((runner) => {
      const horseNumber = formatRunnerNumber(runner.umaban);
      const categoryRows = categoryRowsByHorse.get(horseNumber) ?? {};
      const categoryScores: Record<BloodlineCategory, number> = {
        damSire: categoryRows.damSire ? (rowScores.get(categoryRows.damSire) ?? 0) : 0,
        sire: categoryRows.sire ? (rowScores.get(categoryRows.sire) ?? 0) : 0,
        sireSire: categoryRows.sireSire ? (rowScores.get(categoryRows.sireSire) ?? 0) : 0,
      };
      const rawScore = CATEGORY_ORDER.reduce(
        (total, category) => total + categoryScores[category] * CATEGORY_SCORE_WEIGHTS[category],
        0,
      );
      const categoryRowValues = Object.values(categoryRows).filter(
        (row): row is BloodlineStatsRow => row !== undefined,
      );

      return {
        categoryRows,
        categoryScores,
        horseCount: categoryRowValues.reduce((total, row) => total + row.horseCount, 0),
        horseName: cleanText(runner.bamei),
        horseNumber,
        rawScore,
        starts: categoryRowValues.reduce((total, row) => total + row.starts, 0),
      };
    });

    const maxScore = Math.max(...rawRows.map((row) => row.rawScore), 0);
    const minScore = rawRows.length > 0 ? Math.min(...rawRows.map((row) => row.rawScore)) : 0;
    const scoreRange = maxScore - minScore;

    return rawRows
      .map((row) =>
        Object.assign(row, {
          score: scoreRange > 0 ? (row.rawScore - minScore) / scoreRange : row.rawScore > 0 ? 1 : 0,
        }),
      )
      .toSorted((left, right) =>
        right.score === left.score
          ? Number(left.horseNumber) - Number(right.horseNumber)
          : right.score - left.score,
      );
  }, [rows, runners]);

  const updateParam = (name: string, value: string) => {
    const next = new URLSearchParams(searchParams.toString());
    next.set(name, value);
    router.replace(`?${next.toString()}`, { scroll: false });
  };

  const toggleSetting = (key: ToggleSettingKey) => {
    updateParam(SETTING_PARAMS[key], settings[key] ? "0" : "1");
  };

  const renderConditionToggle = (key: ToggleSettingKey, label: string | null): ReactNode => {
    if (!label || label === "-") {
      return null;
    }

    return (
      <label>
        <input
          checked={settings[key]}
          type="checkbox"
          onChange={() => {
            toggleSetting(key);
          }}
        />
        {label}
      </label>
    );
  };

  const changeSort = (key: RateSortKey) => {
    if (sortKey === key) {
      setSortDirection((current) => (current === "asc" ? "desc" : "asc"));
      return;
    }
    setSortKey(key);
    setSortDirection("desc");
  };

  const renderSortButton = (key: RateSortKey) => {
    const isCurrent = sortKey === key;
    const nextDirection = isCurrent && sortDirection === "desc" ? "昇順" : "降順";

    return (
      <button
        aria-label={`${SORT_LABELS[key]}を${nextDirection}で並び替え`}
        className="stats-sort-button"
        type="button"
        onClick={() => {
          changeSort(key);
        }}
      >
        <span>{SORT_LABELS[key]}</span>
        <small>{isCurrent ? (sortDirection === "desc" ? "降順" : "昇順") : "並替"}</small>
      </button>
    );
  };

  const renderDetailRows = (row: BloodlineStatsRow, colSpan: number) => {
    const sortedDetails = row.details.toSorted(
      (left, right) =>
        right.date.localeCompare(left.date) ||
        right.raceNumber.localeCompare(left.raceNumber) ||
        right.horseNumber.localeCompare(left.horseNumber),
    );

    return (
      <tr className="stats-detail-row">
        <td colSpan={colSpan}>
          <div className="stats-detail-panel">
            <table className="stats-detail-table">
              <thead>
                <tr>
                  <th>父</th>
                  <th>父父</th>
                  <th>母父</th>
                  <th>名前</th>
                  <th>日付</th>
                  <th>競馬場</th>
                  <th>R</th>
                  <th>レース名</th>
                  <th>馬名</th>
                  <th>枠</th>
                  <th>馬番</th>
                  <th>着順</th>
                  <th>レースタイム</th>
                  <th>人気</th>
                  <th>単勝</th>
                </tr>
              </thead>
              <tbody>
                {sortedDetails.map((detail) => (
                  <tr
                    key={`${detail.date}-${detail.keibajoCode}-${detail.raceNumber}-${detail.frameNumber}-${detail.horseNumber}-${detail.rank}`}
                  >
                    <td
                      className={
                        isTargetBloodline(detail.sireName, row.name)
                          ? "bloodline-detail-target"
                          : undefined
                      }
                    >
                      {detail.sireName || "-"}
                    </td>
                    <td
                      className={
                        isTargetBloodline(detail.sireSireName, row.name)
                          ? "bloodline-detail-target"
                          : undefined
                      }
                    >
                      {detail.sireSireName || "-"}
                    </td>
                    <td
                      className={
                        isTargetBloodline(detail.damSireName, row.name)
                          ? "bloodline-detail-target"
                          : undefined
                      }
                    >
                      {detail.damSireName || "-"}
                    </td>
                    <td className="stats-detail-horse-name">{detail.horseName || "-"}</td>
                    <td>{formatDetailDate(detail.date)}</td>
                    <td>{formatKeibajo(detail.keibajoCode)}</td>
                    <td>{formatRaceNumber(detail.raceNumber)}</td>
                    <td className="stats-detail-race-name">{detail.raceName || "-"}</td>
                    <td className="stats-detail-horse-name">{detail.horseName || "-"}</td>
                    <td>{detail.frameNumber || "-"}</td>
                    <td>{detail.horseNumber || "-"}</td>
                    <td>{formatRank(detail.rank)}</td>
                    <td>{formatTenthsTime(detail.raceTime)}</td>
                    <td>{formatRank(detail.popularity)}</td>
                    <td>{formatOdds(detail.winOdds)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </td>
      </tr>
    );
  };

  const renderScoreDetailRows = (row: BloodlineScoreRow, colSpan: number) => {
    const details = CATEGORY_ORDER.flatMap((category) => {
      const categoryRow = row.categoryRows[category];
      if (!categoryRow) {
        return [];
      }
      return categoryRow.details.map((detail) => ({
        bloodlineCategory: CATEGORY_LABELS[category],
        bloodlineName: categoryRow.name,
        category,
        detail,
      }));
    }).toSorted(
      (left, right) =>
        right.detail.date.localeCompare(left.detail.date) ||
        right.detail.raceNumber.localeCompare(left.detail.raceNumber) ||
        right.detail.horseNumber.localeCompare(left.detail.horseNumber) ||
        left.bloodlineCategory.localeCompare(right.bloodlineCategory),
    );

    return (
      <tr className="stats-detail-row">
        <td colSpan={colSpan}>
          <div className="stats-detail-panel">
            <table className="stats-detail-table">
              <thead>
                <tr>
                  <th>父</th>
                  <th>父父</th>
                  <th>母父</th>
                  <th>名前</th>
                  <th>日付</th>
                  <th>競馬場</th>
                  <th>R</th>
                  <th>レース名</th>
                  <th>馬名</th>
                  <th>枠</th>
                  <th>馬番</th>
                  <th>着順</th>
                  <th>レースタイム</th>
                  <th>人気</th>
                  <th>単勝</th>
                </tr>
              </thead>
              <tbody>
                {details.map(({ bloodlineCategory, bloodlineName, detail }) => (
                  <tr
                    key={`${bloodlineCategory}-${bloodlineName}-${detail.date}-${detail.keibajoCode}-${detail.raceNumber}-${detail.frameNumber}-${detail.horseNumber}-${detail.rank}`}
                  >
                    <td
                      className={
                        isTargetBloodline(detail.sireName, bloodlineName)
                          ? "bloodline-detail-target"
                          : undefined
                      }
                    >
                      {detail.sireName || "-"}
                    </td>
                    <td
                      className={
                        isTargetBloodline(detail.sireSireName, bloodlineName)
                          ? "bloodline-detail-target"
                          : undefined
                      }
                    >
                      {detail.sireSireName || "-"}
                    </td>
                    <td
                      className={
                        isTargetBloodline(detail.damSireName, bloodlineName)
                          ? "bloodline-detail-target"
                          : undefined
                      }
                    >
                      {detail.damSireName || "-"}
                    </td>
                    <td className="stats-detail-horse-name">{detail.horseName || "-"}</td>
                    <td>{formatDetailDate(detail.date)}</td>
                    <td>{formatKeibajo(detail.keibajoCode)}</td>
                    <td>{formatRaceNumber(detail.raceNumber)}</td>
                    <td className="stats-detail-race-name">{detail.raceName || "-"}</td>
                    <td className="stats-detail-horse-name">{detail.horseName || "-"}</td>
                    <td>{detail.frameNumber || "-"}</td>
                    <td>{detail.horseNumber || "-"}</td>
                    <td>{formatRank(detail.rank)}</td>
                    <td>{formatTenthsTime(detail.raceTime)}</td>
                    <td>{formatRank(detail.popularity)}</td>
                    <td>{formatOdds(detail.winOdds)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </td>
      </tr>
    );
  };

  const renderStatsTable = (category: BloodlineCategory, categoryRows: BloodlineStatsRow[]) => (
    <section className="stats-category-section" key={category}>
      <div className="section-heading compact">
        <h3>{CATEGORY_TITLE_LABELS[category]}</h3>
      </div>
      {categoryRows.length === 0 ? (
        <p className="empty-state">{CATEGORY_LABELS[category]}別の過去成績はありません。</p>
      ) : (
        <div className="stats-table-wrap">
          <table className="stats-table">
            <colgroup>
              <col className="bloodline-stats-col-horse-number" />
              <col className="bloodline-stats-col-name" />
              <col className="bloodline-stats-col-rate" />
              <col className="bloodline-stats-col-rate" />
              <col className="bloodline-stats-col-rate" />
              <col className="bloodline-stats-col-count" />
              <col className="bloodline-stats-col-count" />
            </colgroup>
            <thead>
              <tr>
                <th>馬番号</th>
                <th>{CATEGORY_LABELS[category]}</th>
                <th>{renderSortButton("showRate")}</th>
                <th>{renderSortButton("quinellaRate")}</th>
                <th>{renderSortButton("winRate")}</th>
                <th>出走回数</th>
                <th>出馬数</th>
              </tr>
            </thead>
            <tbody>
              {categoryRows.map((row) => {
                const rowKey = `${row.category}-${row.name}`;
                const isExpanded = expandedRowKey === rowKey;
                const canExpand = row.starts > 0 && row.details.length > 0;

                return (
                  <Fragment key={rowKey}>
                    <tr className={isExpanded ? "stats-row-expanded" : undefined}>
                      <td>{row.currentHorseNumbers}</td>
                      <td className="stats-name-cell">
                        {canExpand ? (
                          <button
                            aria-expanded={isExpanded}
                            className="stats-detail-toggle"
                            type="button"
                            onClick={() => {
                              setExpandedRowKey((current) => (current === rowKey ? null : rowKey));
                            }}
                          >
                            {row.name}
                          </button>
                        ) : (
                          row.name
                        )}
                      </td>
                      <td>{formatRate(row.showRate)}</td>
                      <td>{formatRate(row.quinellaRate)}</td>
                      <td>{formatRate(row.winRate)}</td>
                      <td>{row.starts.toLocaleString("ja-JP")}</td>
                      <td>{row.horseCount.toLocaleString("ja-JP")}</td>
                    </tr>
                    {isExpanded ? renderDetailRows(row, 7) : null}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );

  return (
    <>
      <MobileFilterDisclosure title="条件設定">
        <section className="stats-control-panel" aria-label="bloodline stats controls">
          <label>
            <span>期間</span>
            <select
              value={settings.years === null ? "all" : String(settings.years)}
              onChange={(event) => {
                updateParam("statsYears", event.currentTarget.value);
              }}
            >
              {[1, 2, 3, 5, 10].map((year) => (
                <option value={year} key={year}>
                  {year}年
                </option>
              ))}
              <option value="all">全期間</option>
            </select>
          </label>
          {renderConditionToggle("includeVenue", conditionLabels.venue)}
          {renderConditionToggle("includeMonthWindow", conditionLabels.monthWindow)}
          {renderConditionToggle("includeRaceTitle", conditionLabels.raceTitle)}
          {renderConditionToggle("includeRaceSubtitle", conditionLabels.raceSubtitle)}
          {renderConditionToggle("includeAge", conditionLabels.age)}
          {renderConditionToggle("includeClass", conditionLabels.class)}
          {renderConditionToggle("includeSex", conditionLabels.sex)}
          {renderConditionToggle("includeWeight", conditionLabels.weight)}
          {renderConditionToggle("includeSurface", conditionLabels.surface)}
          {renderConditionToggle("includeTurn", conditionLabels.turn)}
          {renderConditionToggle("includeDistance", conditionLabels.distance)}
          {renderConditionToggle("includeFrame", conditionLabels.frame)}
          {renderConditionToggle("includeRaceNumber", conditionLabels.raceNumber)}
        </section>
      </MobileFilterDisclosure>

      <div className="stats-category-list">
        <section className="stats-category-section">
          <div className="section-heading compact">
            <h3>血統スコア</h3>
          </div>
          <div className="stats-table-wrap">
            <table className="stats-table bloodline-score-table">
              <colgroup>
                <col className="bloodline-stats-col-horse-number" />
                <col className="bloodline-score-col-horse-name" />
                <col className="bloodline-score-col-score" />
                <col className="bloodline-stats-col-count" />
                <col className="bloodline-stats-col-count" />
                <col className="bloodline-score-col-name" />
                <col className="bloodline-score-col-score" />
                <col className="bloodline-score-col-name" />
                <col className="bloodline-score-col-score" />
                <col className="bloodline-score-col-name" />
                <col className="bloodline-score-col-score" />
              </colgroup>
              <thead>
                <tr>
                  <th>馬番号</th>
                  <th>馬名</th>
                  <th>スコア</th>
                  <th>出走数</th>
                  <th>出馬数</th>
                  <th>父</th>
                  <th>父スコア</th>
                  <th>母父</th>
                  <th>母父スコア</th>
                  <th>父父</th>
                  <th>父父スコア</th>
                </tr>
              </thead>
              <tbody>
                {scoreRows.map((row) => {
                  const isExpanded = expandedScoreRowKey === row.horseNumber;
                  const canExpand = CATEGORY_ORDER.some(
                    (category) => (row.categoryRows[category]?.details.length ?? 0) > 0,
                  );

                  return (
                    <Fragment key={row.horseNumber}>
                      <tr className={isExpanded ? "stats-row-expanded" : undefined}>
                        <td>{row.horseNumber}</td>
                        <td className="stats-name-cell">
                          {canExpand ? (
                            <button
                              aria-expanded={isExpanded}
                              className="stats-detail-toggle"
                              type="button"
                              onClick={() => {
                                setExpandedScoreRowKey((current) =>
                                  current === row.horseNumber ? null : row.horseNumber,
                                );
                              }}
                            >
                              {row.horseName}
                            </button>
                          ) : (
                            row.horseName
                          )}
                        </td>
                        <td className="bloodline-score-cell">{formatScore(row.score)}</td>
                        <td>{row.starts.toLocaleString("ja-JP")}</td>
                        <td>{row.horseCount.toLocaleString("ja-JP")}</td>
                        <td className="stats-name-cell">{row.categoryRows.sire?.name ?? "-"}</td>
                        <td>{formatScore(row.categoryScores.sire)}</td>
                        <td className="stats-name-cell">{row.categoryRows.damSire?.name ?? "-"}</td>
                        <td>{formatScore(row.categoryScores.damSire)}</td>
                        <td className="stats-name-cell">
                          {row.categoryRows.sireSire?.name ?? "-"}
                        </td>
                        <td>{formatScore(row.categoryScores.sireSire)}</td>
                      </tr>
                      {isExpanded ? renderScoreDetailRows(row, 11) : null}
                    </Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        </section>
        {groupedRows.map(({ category, rows: categoryRows }) =>
          renderStatsTable(category, categoryRows),
        )}
      </div>
    </>
  );
}
