"use client";

import { useRouter, useSearchParams } from "next/navigation";
import type { ReactNode } from "react";
import { Fragment, useMemo, useState } from "react";

import { formatDate, formatKeibajo, formatRaceNumber } from "../../../lib/format";
import type { SimilarRaceStatsRow, SimilarRaceStatsSettings } from "../../../lib/race-types";
import { MobileFilterDisclosure } from "./mobile-filter-disclosure";

type RateSortKey = "score" | "showRate" | "quinellaRate" | "winRate";
type SortDirection = "asc" | "desc";
type StatsCategory = SimilarRaceStatsRow["category"];

interface ScoredSimilarRaceStatsRow extends SimilarRaceStatsRow {
  score: number;
}

interface SimilarRaceStatsTableProps {
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
  rows: SimilarRaceStatsRow[];
  settings: SimilarRaceStatsSettings;
}

const SORT_LABELS: Record<RateSortKey, string> = {
  quinellaRate: "連対率",
  score: "スコア",
  showRate: "複勝率",
  winRate: "勝率",
};

const CATEGORY_LABELS: Record<StatsCategory, string> = {
  jockey: "騎手",
  owner: "馬主",
  trainer: "調教師",
};

const CATEGORY_TITLE_LABELS: Record<StatsCategory, string> = {
  jockey: "騎手の勝率",
  owner: "馬主の勝率",
  trainer: "調教師の勝率",
};

const CATEGORY_ORDER: StatsCategory[] = ["jockey", "trainer", "owner"];

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

const normalize = (value: number, max: number): number => (max > 0 ? value / max : 0);

const calculateScore = (
  row: SimilarRaceStatsRow,
  maxStarts: number,
  maxHorseCount: number,
): number =>
  (row.showRate / 100) * METRIC_SCORE_WEIGHTS.showRate +
  (row.quinellaRate / 100) * METRIC_SCORE_WEIGHTS.quinellaRate +
  (row.winRate / 100) * METRIC_SCORE_WEIGHTS.winRate +
  normalize(row.starts, maxStarts) * METRIC_SCORE_WEIGHTS.starts +
  normalize(row.horseCount, maxHorseCount) * METRIC_SCORE_WEIGHTS.horseCount;

const toScoredRows = (rows: SimilarRaceStatsRow[]): ScoredSimilarRaceStatsRow[] => {
  const maxStarts = Math.max(...rows.map((row) => row.starts), 0);
  const maxHorseCount = Math.max(...rows.map((row) => row.horseCount), 0);
  const rawRows = rows.map((row) => ({
    rawScore: calculateScore(row, maxStarts, maxHorseCount),
    row,
  }));
  const maxScore = Math.max(...rawRows.map((row) => row.rawScore), 0);
  const minScore = rawRows.length > 0 ? Math.min(...rawRows.map((row) => row.rawScore)) : 0;
  const scoreRange = maxScore - minScore;

  return rawRows.map(({ rawScore, row }) =>
    Object.assign({}, row, {
      score: scoreRange > 0 ? (rawScore - minScore) / scoreRange : rawScore > 0 ? 1 : 0,
    }),
  );
};

export function SimilarRaceStatsTable({
  conditionLabels,
  rows,
  settings,
}: SimilarRaceStatsTableProps) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [sortKey, setSortKey] = useState<RateSortKey>("score");
  const [sortDirection, setSortDirection] = useState<SortDirection>("desc");
  const [expandedRowKey, setExpandedRowKey] = useState<string | null>(null);

  const groupedRows = useMemo(() => {
    return CATEGORY_ORDER.map((category) => ({
      category,
      rows: toScoredRows(rows.filter((row) => row.category === category)).toSorted(
        (left, right) => {
          const compared =
            sortDirection === "asc"
              ? left[sortKey] - right[sortKey]
              : right[sortKey] - left[sortKey];
          return compared === 0 ? right.starts - left.starts : compared;
        },
      ),
    }));
  }, [rows, sortDirection, sortKey]);

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

  const renderDetailRows = (row: SimilarRaceStatsRow, colSpan: number) => {
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

  const renderStatsTable = (category: StatsCategory, categoryRows: ScoredSimilarRaceStatsRow[]) => (
    <section className="stats-category-section" key={category}>
      <div className="section-heading compact">
        <h3>{CATEGORY_TITLE_LABELS[category]}</h3>
      </div>
      {categoryRows.length === 0 ? (
        <p className="empty-state">{CATEGORY_LABELS[category]}別の過去成績はありません。</p>
      ) : (
        <div className="stats-table-wrap">
          <table className="stats-table">
            <thead>
              <tr>
                <th>馬番号</th>
                <th>名前</th>
                <th>{renderSortButton("score")}</th>
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
                      <td className="stats-score-cell">{formatScore(row.score)}</td>
                      <td>{formatRate(row.showRate)}</td>
                      <td>{formatRate(row.quinellaRate)}</td>
                      <td>{formatRate(row.winRate)}</td>
                      <td>{row.starts.toLocaleString("ja-JP")}</td>
                      <td>{row.horseCount.toLocaleString("ja-JP")}</td>
                    </tr>
                    {isExpanded ? renderDetailRows(row, 8) : null}
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
        <section className="stats-control-panel" aria-label="similar race stats controls">
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
        {groupedRows.map(({ category, rows: categoryRows }) =>
          renderStatsTable(category, categoryRows),
        )}
      </div>
    </>
  );
}
