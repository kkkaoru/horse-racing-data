"use client";

import { useRouter, useSearchParams } from "next/navigation";
import type { ReactNode } from "react";
import { useMemo, useState } from "react";

import type { SimilarRaceStatsRow, SimilarRaceStatsSettings } from "../../../lib/race-types";

type RateSortKey = "showRate" | "quinellaRate" | "winRate";
type SortDirection = "asc" | "desc";
type StatsCategory = SimilarRaceStatsRow["category"];

interface SimilarRaceStatsTableProps {
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
  rows: SimilarRaceStatsRow[];
  settings: SimilarRaceStatsSettings;
}

const SORT_LABELS: Record<RateSortKey, string> = {
  quinellaRate: "連対率",
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

export function SimilarRaceStatsTable({
  conditionLabels,
  rows,
  settings,
}: SimilarRaceStatsTableProps) {
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

  const renderStatsTable = (category: StatsCategory, categoryRows: SimilarRaceStatsRow[]) => (
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
                <th>名前</th>
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
        {groupedRows.map(({ category, rows: categoryRows }) =>
          renderStatsTable(category, categoryRows),
        )}
      </div>
    </>
  );
}
