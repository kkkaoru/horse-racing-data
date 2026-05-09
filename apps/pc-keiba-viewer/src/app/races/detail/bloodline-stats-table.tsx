"use client";

import { useRouter, useSearchParams } from "next/navigation";
import type { ReactNode } from "react";
import { useMemo, useState } from "react";

import { cleanText } from "../../../lib/format";
import type { BloodlineStatsRow, Runner, SimilarRaceStatsSettings } from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";

type BloodlineCategory = BloodlineStatsRow["category"];
type RateSortKey = "showRate" | "quinellaRate" | "winRate";
type SortDirection = "asc" | "desc";

interface BloodlineStatsTableProps {
  conditionLabels: {
    age: string | null;
    class: string | null;
    distance: string | null;
    frame: string;
    raceNumber: string;
    raceSubtitle: string | null;
    raceTitle: string | null;
    sex: string | null;
    surface: string | null;
    turn: string | null;
    venue: string | null;
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

type ToggleSettingKey = keyof Omit<SimilarRaceStatsSettings, "classConditionName" | "years">;

const SETTING_PARAMS: Record<ToggleSettingKey, string> = {
  includeAge: "statsAge",
  includeClass: "statsClass",
  includeDistance: "statsDistance",
  includeFrame: "statsFrame",
  includeRaceNumber: "statsRaceNumber",
  includeRaceSubtitle: "statsRaceSubtitle",
  includeRaceTitle: "statsRaceTitle",
  includeSex: "statsSex",
  includeSurface: "statsSurface",
  includeTurn: "statsTurn",
  includeVenue: "statsVenue",
};

const formatRate = (value: number): string => `${value.toFixed(1)}%`;

const formatScore = (value: number): string => value.toFixed(2);

const splitHorseNumbers = (value: string): string[] =>
  value
    .split(",")
    .map((horseNumber) => cleanText(horseNumber, ""))
    .filter(Boolean);

const normalize = (value: number, max: number): number => (max > 0 ? value / max : 0);

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

      return {
        categoryRows,
        categoryScores,
        horseName: cleanText(runner.bamei),
        horseNumber,
        rawScore,
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

  const renderStatsTable = (category: BloodlineCategory, categoryRows: BloodlineStatsRow[]) => (
    <section className="stats-category-section" key={category}>
      <div className="section-heading compact">
        <h3>{CATEGORY_TITLE_LABELS[category]}</h3>
        <span>{categoryRows.length} 件</span>
      </div>
      {categoryRows.length === 0 ? (
        <p className="empty-state">{CATEGORY_LABELS[category]}別の過去成績はありません。</p>
      ) : (
        <div className="stats-table-wrap">
          <table className="stats-table">
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
              {categoryRows.map((row) => (
                <tr key={`${row.category}-${row.name}`}>
                  <td>{row.currentHorseNumbers}</td>
                  <td className="stats-name-cell">{row.name}</td>
                  <td>{formatRate(row.showRate)}</td>
                  <td>{formatRate(row.quinellaRate)}</td>
                  <td>{formatRate(row.winRate)}</td>
                  <td>{row.starts.toLocaleString("ja-JP")}</td>
                  <td>{row.horseCount.toLocaleString("ja-JP")}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );

  return (
    <>
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
        {renderConditionToggle("includeRaceTitle", conditionLabels.raceTitle)}
        {renderConditionToggle("includeRaceSubtitle", conditionLabels.raceSubtitle)}
        {renderConditionToggle("includeAge", conditionLabels.age)}
        {renderConditionToggle("includeClass", conditionLabels.class)}
        {renderConditionToggle("includeSex", conditionLabels.sex)}
        {renderConditionToggle("includeSurface", conditionLabels.surface)}
        {renderConditionToggle("includeTurn", conditionLabels.turn)}
        {renderConditionToggle("includeDistance", conditionLabels.distance)}
        {renderConditionToggle("includeFrame", conditionLabels.frame)}
        {renderConditionToggle("includeRaceNumber", conditionLabels.raceNumber)}
      </section>

      <div className="stats-category-list">
        <section className="stats-category-section">
          <div className="section-heading compact">
            <h3>血統スコア</h3>
            <span>{scoreRows.length} 頭</span>
          </div>
          <div className="stats-table-wrap">
            <table className="stats-table bloodline-score-table">
              <thead>
                <tr>
                  <th>馬番号</th>
                  <th>馬名</th>
                  <th>スコア</th>
                  <th>父</th>
                  <th>父スコア</th>
                  <th>母父</th>
                  <th>母父スコア</th>
                  <th>父父</th>
                  <th>父父スコア</th>
                </tr>
              </thead>
              <tbody>
                {scoreRows.map((row) => (
                  <tr key={row.horseNumber}>
                    <td>{row.horseNumber}</td>
                    <td className="stats-name-cell">{row.horseName}</td>
                    <td className="bloodline-score-cell">{formatScore(row.score)}</td>
                    <td className="stats-name-cell">{row.categoryRows.sire?.name ?? "-"}</td>
                    <td>{formatScore(row.categoryScores.sire)}</td>
                    <td className="stats-name-cell">{row.categoryRows.damSire?.name ?? "-"}</td>
                    <td>{formatScore(row.categoryScores.damSire)}</td>
                    <td className="stats-name-cell">{row.categoryRows.sireSire?.name ?? "-"}</td>
                    <td>{formatScore(row.categoryScores.sireSire)}</td>
                  </tr>
                ))}
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
