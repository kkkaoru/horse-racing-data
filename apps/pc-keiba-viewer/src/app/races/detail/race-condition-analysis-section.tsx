"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import type { ReactNode } from "react";
import { Fragment, memo, useEffect, useState } from "react";

import type { RaceSource } from "../../../lib/codes";
import { formatDate, formatKeibajo, formatRaceNumber } from "../../../lib/format";
import type {
  FinishPositionStatsRow,
  FrameStatsRow,
  PayoutStatsRow,
  RaceTimeStats,
  Runner,
  SimilarRaceStatsSettings,
  StatsDetail,
} from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";
import { FrameNumberBadge } from "./frame-number-badge";
import { MobileFilterDisclosure } from "./mobile-filter-disclosure";

interface RaceConditionAnalysisSectionProps {
  conditionLabels: {
    age: string | null;
    class: string | null;
    distance: string | null;
    frame: string;
    monthWindow: string;
    raceNumber: string;
    raceSubtitle: string | null;
    raceTitle: string | null;
    runnerCount: string | null;
    sex: string | null;
    surface: string | null;
    turn: string | null;
    venue: string | null;
    weight: string | null;
  };
  frameStats: FrameStatsRow[];
  finishPositionStats: FinishPositionStatsRow[];
  payoutStats: PayoutStatsRow[];
  raceTimeStats: RaceTimeStats;
  runners: Runner[];
  settings: SimilarRaceStatsSettings;
  source: RaceSource;
}

type ToggleSettingKey = keyof Omit<
  SimilarRaceStatsSettings,
  | "classConditionName"
  | "includeBloodlineAncestors"
  | "includeNarOnly"
  | "runnerCount"
  | "sourceScope"
  | "years"
>;

const SETTING_PARAMS: Record<ToggleSettingKey, string> = {
  includeAge: "analysisStatsAge",
  includeClass: "analysisStatsClass",
  includeDistance: "analysisStatsDistance",
  includeFrame: "analysisStatsFrame",
  includeMonthWindow: "analysisStatsMonthWindow",
  includeRaceNumber: "analysisStatsRaceNumber",
  includeRaceSubtitle: "analysisStatsRaceSubtitle",
  includeRaceTitle: "analysisStatsRaceTitle",
  includeRunnerCount: "analysisStatsRunnerCount",
  includeSex: "analysisStatsSex",
  includeSurface: "analysisStatsSurface",
  includeTurn: "analysisStatsTurn",
  includeVenue: "analysisStatsVenue",
  includeWeight: "analysisStatsWeight",
};

const TARGET_RACE_PAGE_SIZE_OPTIONS = [5, 10, 20, 50];

const ALL_CONDITION_KEYS: ToggleSettingKey[] = [
  "includeAge",
  "includeClass",
  "includeFrame",
  "includeMonthWindow",
  "includeRaceNumber",
  "includeRaceSubtitle",
  "includeRaceTitle",
  "includeRunnerCount",
  "includeSex",
  "includeSurface",
  "includeTurn",
  "includeWeight",
];

const formatDetailDate = (date: string): string =>
  date.length === 8 ? formatDate(date.slice(0, 4), date.slice(4, 8)) : "-";

const formatTenthsTime = (value: number | null): string => {
  if (value === null) {
    return "-";
  }
  const tenths = Math.round(value);
  const minutes = Math.floor(tenths / 600);
  const seconds = Math.floor((tenths % 600) / 10);
  const remainder = tenths % 10;
  return minutes > 0
    ? `${minutes}:${String(seconds).padStart(2, "0")}.${remainder}`
    : `${seconds}.${remainder}`;
};

const formatDecimalTenths = (value: number | null): string =>
  value === null ? "-" : (value / 10).toFixed(1);

const formatNumber = (value: number | null, digits = 1): string =>
  value === null ? "-" : value.toFixed(digits);

const formatYen = (value: number | null): string =>
  value === null ? "-" : `${Math.round(value).toLocaleString("ja-JP")}円`;

const parseRank = (value: string): string => {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? String(parsed) : "-";
};

const parseOdds = (value: string): string => {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? (parsed / 10).toFixed(1) : "-";
};

const parseTenths = (value: string): number | null => {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
};

const buildRaceHref = (date: string, keibajoCode: string, raceNumber: string): string => {
  const year = date.slice(0, 4);
  const month = date.slice(4, 6);
  const day = date.slice(6, 8);
  return `/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}`;
};

const renderFastestDetail = (detail: StatsDetail | null) => {
  if (!detail) {
    return <p className="empty-state">該当する最速レースはありません。</p>;
  }

  return (
    <div className="stats-table-wrap analysis-detail-wrap">
      <table className="stats-table analysis-detail-table">
        <thead>
          <tr>
            <th>日付</th>
            <th>競馬場</th>
            <th>R</th>
            <th>レース名</th>
            <th>馬名</th>
            <th>騎手</th>
            <th>枠</th>
            <th>馬番</th>
            <th>着順</th>
            <th>レースタイム</th>
            <th>人気</th>
            <th>単勝</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>{formatDetailDate(detail.date)}</td>
            <td>{formatKeibajo(detail.keibajoCode)}</td>
            <td>{formatRaceNumber(detail.raceNumber)}</td>
            <td className="stats-name-cell">{detail.raceName || "-"}</td>
            <td className="stats-name-cell">{detail.horseName || "-"}</td>
            <td>{detail.jockeyName || "-"}</td>
            <td>
              <FrameNumberBadge value={detail.frameNumber} />
            </td>
            <td>{detail.horseNumber || "-"}</td>
            <td>{parseRank(detail.rank)}</td>
            <td>{formatTenthsTime(parseTenths(detail.raceTime))}</td>
            <td>{parseRank(detail.popularity)}</td>
            <td>{parseOdds(detail.winOdds)}</td>
          </tr>
        </tbody>
      </table>
    </div>
  );
};

export const RaceConditionAnalysisSection = memo(function RaceConditionAnalysisSection({
  conditionLabels,
  frameStats,
  finishPositionStats,
  payoutStats,
  raceTimeStats,
  runners,
  settings,
  source,
}: RaceConditionAnalysisSectionProps) {
  const router = useRouter();
  const [displaySettings, setDisplaySettings] = useState(settings);
  const [expandedPayoutKey, setExpandedPayoutKey] = useState<string | null>(null);
  const [expandedFinishKey, setExpandedFinishKey] = useState<number | null>(null);
  const [expandedFrameKey, setExpandedFrameKey] = useState<string | null>(null);
  const [targetRacePage, setTargetRacePage] = useState(1);
  const [targetRacePageSize, setTargetRacePageSize] = useState(5);

  useEffect(() => {
    setDisplaySettings(settings);
  }, [settings]);

  useEffect(() => {
    setTargetRacePage(1);
  }, [raceTimeStats.targetRaces, targetRacePageSize]);

  const replaceParams = (next: URLSearchParams) => {
    const scrollY = window.scrollY;
    router.replace(`?${next.toString()}`, { scroll: false });
    window.requestAnimationFrame(() => {
      window.scrollTo({ top: scrollY });
    });
  };

  const updateParam = (name: string, value: string) => {
    const next = new URLSearchParams(window.location.search);
    next.set(name, value);
    replaceParams(next);
  };

  const toggleSetting = (key: ToggleSettingKey) => {
    const enabled = !displaySettings[key];
    setDisplaySettings((current) => ({ ...current, [key]: enabled }));
    updateParam(SETTING_PARAMS[key], enabled ? "1" : "0");
  };

  const clearConditionSettings = () => {
    const next = new URLSearchParams(window.location.search);
    for (const key of ALL_CONDITION_KEYS) {
      next.set(SETTING_PARAMS[key], "0");
    }
    setDisplaySettings((current) => ({
      ...current,
      includeAge: false,
      includeClass: false,
      includeFrame: false,
      includeMonthWindow: false,
      includeRaceNumber: false,
      includeRaceSubtitle: false,
      includeRaceTitle: false,
      includeRunnerCount: false,
      includeSex: false,
      includeSurface: false,
      includeTurn: false,
      includeWeight: false,
    }));
    replaceParams(next);
  };
  const sourceScopeChecked = displaySettings.sourceScope === source;
  const sourceScopeLabel = source === "jra" ? "中央競馬のみ" : "地方競馬のみ";

  const renderConditionToggle = (key: ToggleSettingKey, label: string | null): ReactNode => {
    if (!label || label === "-") {
      return null;
    }

    return (
      <label>
        <input
          checked={displaySettings[key]}
          type="checkbox"
          onChange={() => {
            toggleSetting(key);
          }}
        />
        {label}
      </label>
    );
  };
  const sortedFrameStats = frameStats.toSorted(
    (left, right) =>
      right.score - left.score ||
      right.count - left.count ||
      left.frameNumber.localeCompare(right.frameNumber),
  );
  const runnerNumbersByFrame = new Map<string, string[]>();
  for (const runner of runners) {
    const frameNumber = runner.wakuban?.trim();
    const horseNumber = formatRunnerNumber(runner.umaban);
    if (!frameNumber || horseNumber === "-") {
      continue;
    }
    runnerNumbersByFrame.set(frameNumber, [
      ...(runnerNumbersByFrame.get(frameNumber) ?? []),
      horseNumber,
    ]);
  }
  const targetRaceTotalPages = Math.max(
    1,
    Math.ceil(raceTimeStats.targetRaces.length / targetRacePageSize),
  );
  const normalizedTargetRacePage = Math.min(targetRacePage, targetRaceTotalPages);
  const targetRaceStartIndex = (normalizedTargetRacePage - 1) * targetRacePageSize;
  const visibleTargetRaces = raceTimeStats.targetRaces.slice(
    targetRaceStartIndex,
    targetRaceStartIndex + targetRacePageSize,
  );

  return (
    <>
      <MobileFilterDisclosure title="条件設定">
        <section className="stats-control-panel" aria-label="race condition analysis controls">
          <label>
            <span>期間</span>
            <select
              value={displaySettings.years === null ? "all" : String(displaySettings.years)}
              onChange={(event) => {
                const years =
                  event.currentTarget.value === "all" ? null : Number(event.currentTarget.value);
                setDisplaySettings((current) => ({ ...current, years }));
                updateParam("analysisStatsYears", event.currentTarget.value);
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
          <label>
            <input
              checked={sourceScopeChecked}
              type="checkbox"
              onChange={() => {
                const sourceScope = sourceScopeChecked ? "all" : source;
                setDisplaySettings((current) => ({ ...current, sourceScope }));
                updateParam("analysisStatsSourceScope", sourceScope);
              }}
            />
            {sourceScopeLabel}
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
          {renderConditionToggle("includeRunnerCount", conditionLabels.runnerCount)}
          {renderConditionToggle("includeFrame", conditionLabels.frame)}
          {renderConditionToggle("includeRaceNumber", conditionLabels.raceNumber)}
          <button className="stats-control-button" type="button" onClick={clearConditionSettings}>
            全ての条件を外す
          </button>
        </section>
      </MobileFilterDisclosure>

      <div className="stats-category-list">
        <section className="stats-category-section">
          <div className="section-heading compact">
            <h3>タイム傾向</h3>
          </div>
          <div className="analysis-metric-grid">
            <div>
              <span>最速レースタイム</span>
              <strong>{formatTenthsTime(raceTimeStats.fastestRaceTime)}</strong>
            </div>
            <div>
              <span>最速レースの上がり3F</span>
              <strong>{formatDecimalTenths(raceTimeStats.fastestKohan3f)}</strong>
            </div>
            <div>
              <span>平均レースタイム</span>
              <strong>{formatTenthsTime(raceTimeStats.averageRaceTime)}</strong>
            </div>
            <div>
              <span>平均上がり3F</span>
              <strong>{formatDecimalTenths(raceTimeStats.averageKohan3f)}</strong>
            </div>
            <div>
              <span>中央値レースタイム</span>
              <strong>{formatTenthsTime(raceTimeStats.medianRaceTime)}</strong>
            </div>
            <div>
              <span>中央値上がり3F</span>
              <strong>{formatDecimalTenths(raceTimeStats.medianKohan3f)}</strong>
            </div>
          </div>
          {renderFastestDetail(raceTimeStats.fastestDetail)}
        </section>

        <section className="stats-category-section">
          <div className="section-heading compact">
            <h3>対象レース一覧</h3>
          </div>
          <div className="analysis-pagination-controls">
            <label>
              <span>表示件数</span>
              <select
                value={targetRacePageSize}
                onChange={(event) => {
                  setTargetRacePageSize(Number(event.currentTarget.value));
                }}
              >
                {TARGET_RACE_PAGE_SIZE_OPTIONS.map((pageSize) => (
                  <option key={pageSize} value={pageSize}>
                    {pageSize}件
                  </option>
                ))}
              </select>
            </label>
            <div>
              <button
                disabled={normalizedTargetRacePage <= 1}
                type="button"
                onClick={() => {
                  setTargetRacePage((current) => Math.max(1, current - 1));
                }}
              >
                前へ
              </button>
              <span>
                {normalizedTargetRacePage} / {targetRaceTotalPages}
              </span>
              <button
                disabled={normalizedTargetRacePage >= targetRaceTotalPages}
                type="button"
                onClick={() => {
                  setTargetRacePage((current) => Math.min(targetRaceTotalPages, current + 1));
                }}
              >
                次へ
              </button>
            </div>
          </div>
          <div className="stats-table-wrap">
            <table className="stats-table analysis-table">
              <thead>
                <tr>
                  <th>日付</th>
                  <th>競馬場</th>
                  <th>R</th>
                  <th>レース名</th>
                  <th>1着馬番</th>
                  <th>1着馬名</th>
                  <th>騎手</th>
                  <th>調教師</th>
                  <th>馬主</th>
                  <th>レースタイム</th>
                  <th>上がり3F</th>
                  <th>人気</th>
                </tr>
              </thead>
              <tbody>
                {visibleTargetRaces.length > 0 ? (
                  visibleTargetRaces.map((targetRace) => (
                    <tr
                      key={`${targetRace.date}-${targetRace.keibajoCode}-${targetRace.raceNumber}-${targetRace.horseNumber}`}
                    >
                      <td>{formatDetailDate(targetRace.date)}</td>
                      <td>{formatKeibajo(targetRace.keibajoCode)}</td>
                      <td>{formatRaceNumber(targetRace.raceNumber)}</td>
                      <td className="stats-name-cell">
                        <Link
                          href={buildRaceHref(
                            targetRace.date,
                            targetRace.keibajoCode,
                            targetRace.raceNumber,
                          )}
                        >
                          {targetRace.raceName || "一般競走"}
                        </Link>
                      </td>
                      <td>{formatRunnerNumber(targetRace.horseNumber)}</td>
                      <td className="stats-name-cell">{targetRace.horseName || "-"}</td>
                      <td>{targetRace.jockeyName || "-"}</td>
                      <td>{targetRace.trainerName || "-"}</td>
                      <td className="stats-name-cell">{targetRace.ownerName || "-"}</td>
                      <td>{formatTenthsTime(parseTenths(targetRace.raceTime))}</td>
                      <td>{formatDecimalTenths(parseTenths(targetRace.kohan3f))}</td>
                      <td>{parseRank(targetRace.popularity)}</td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={12}>対象レースはありません。</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>

        <section className="stats-category-section">
          <div className="section-heading compact">
            <h3>枠順分析</h3>
          </div>
          <div className="stats-table-wrap">
            <table className="stats-table analysis-table">
              <thead>
                <tr>
                  <th>枠</th>
                  <th>該当馬番</th>
                  <th>頭数</th>
                  <th>スコア</th>
                  <th>着順平均</th>
                  <th>着順中央値</th>
                  <th>人気平均</th>
                  <th>人気中央値</th>
                </tr>
              </thead>
              <tbody>
                {sortedFrameStats.map((row) => {
                  const isExpanded = expandedFrameKey === row.frameNumber;
                  const runnerCountLabel =
                    settings.includeRunnerCount && row.runnerCount !== null
                      ? `${row.runnerCount}頭`
                      : "-";
                  const runnerNumbers =
                    runnerNumbersByFrame.get(row.frameNumber)?.join(", ") ?? "-";

                  return (
                    <Fragment key={row.frameNumber}>
                      <tr className={isExpanded ? "stats-row-expanded" : undefined}>
                        <td className="stats-name-cell">
                          {row.details.length > 0 ? (
                            <button
                              aria-expanded={isExpanded}
                              aria-label={`${row.frameNumber}枠の詳細を${isExpanded ? "閉じる" : "開く"}`}
                              className="stats-detail-toggle"
                              type="button"
                              onClick={() => {
                                setExpandedFrameKey((current) =>
                                  current === row.frameNumber ? null : row.frameNumber,
                                );
                              }}
                            >
                              <FrameNumberBadge value={row.frameNumber} />
                            </button>
                          ) : (
                            <FrameNumberBadge value={row.frameNumber} />
                          )}
                        </td>
                        <td>{runnerNumbers}</td>
                        <td>{runnerCountLabel}</td>
                        <td className="stats-score-cell">{row.score.toFixed(2)}</td>
                        <td>{formatNumber(row.averageFinish)}</td>
                        <td>{formatNumber(row.medianFinish)}</td>
                        <td>{formatNumber(row.averagePopularity)}</td>
                        <td>{formatNumber(row.medianPopularity)}</td>
                      </tr>
                      {isExpanded ? (
                        <tr className="stats-detail-row">
                          <td colSpan={8}>
                            <div className="stats-detail-panel">
                              <table className="stats-detail-table">
                                <thead>
                                  <tr>
                                    <th>日付</th>
                                    <th>競馬場</th>
                                    <th>R</th>
                                    <th>レース名</th>
                                    <th>馬名</th>
                                    <th>騎手</th>
                                    <th>枠</th>
                                    <th>馬番</th>
                                    <th>着順</th>
                                    <th>レースタイム</th>
                                    <th>人気</th>
                                    <th>単勝</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {row.details.map((detail) => (
                                    <tr
                                      key={`${detail.date}-${detail.keibajoCode}-${detail.raceNumber}-${detail.frameNumber}-${detail.horseNumber}-${detail.rank}`}
                                    >
                                      <td>{formatDetailDate(detail.date)}</td>
                                      <td>{formatKeibajo(detail.keibajoCode)}</td>
                                      <td>{formatRaceNumber(detail.raceNumber)}</td>
                                      <td className="stats-detail-race-name">
                                        {detail.raceName || "-"}
                                      </td>
                                      <td className="stats-detail-horse-name">
                                        {detail.horseName || "-"}
                                      </td>
                                      <td>{detail.jockeyName || "-"}</td>
                                      <td>
                                        <FrameNumberBadge value={detail.frameNumber} />
                                      </td>
                                      <td>{detail.horseNumber || "-"}</td>
                                      <td>{parseRank(detail.rank)}</td>
                                      <td>{formatTenthsTime(parseTenths(detail.raceTime))}</td>
                                      <td>{parseRank(detail.popularity)}</td>
                                      <td>{parseOdds(detail.winOdds)}</td>
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
        </section>

        <section className="stats-category-section">
          <div className="section-heading compact">
            <h3>払い戻し傾向</h3>
          </div>
          <div className="stats-table-wrap">
            <table className="stats-table analysis-table">
              <thead>
                <tr>
                  <th>馬券</th>
                  <th>最小</th>
                  <th>最大</th>
                  <th>平均</th>
                  <th>中央値</th>
                  <th>件数</th>
                </tr>
              </thead>
              <tbody>
                {payoutStats.map((row) => {
                  const isExpanded = expandedPayoutKey === row.betType;

                  return (
                    <Fragment key={row.betType}>
                      <tr className={isExpanded ? "stats-row-expanded" : undefined}>
                        <td className="stats-name-cell">
                          {row.details.length > 0 ? (
                            <button
                              aria-expanded={isExpanded}
                              className="stats-detail-toggle"
                              type="button"
                              onClick={() => {
                                setExpandedPayoutKey((current) =>
                                  current === row.betType ? null : row.betType,
                                );
                              }}
                            >
                              {row.betType}
                            </button>
                          ) : (
                            row.betType
                          )}
                        </td>
                        <td>{formatYen(row.minPayout)}</td>
                        <td>{formatYen(row.maxPayout)}</td>
                        <td>{formatYen(row.averagePayout)}</td>
                        <td>{formatYen(row.medianPayout)}</td>
                        <td>{row.count.toLocaleString("ja-JP")}</td>
                      </tr>
                      {isExpanded ? (
                        <tr className="stats-detail-row">
                          <td colSpan={6}>
                            <div className="stats-detail-panel">
                              <table className="stats-detail-table analysis-payout-detail-table">
                                <thead>
                                  <tr>
                                    <th>日付</th>
                                    <th>競馬場</th>
                                    <th>R</th>
                                    <th>レース名</th>
                                    <th>払戻</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {row.details.map((detail) => (
                                    <tr
                                      key={`${detail.date}-${detail.keibajoCode}-${detail.raceNumber}-${detail.payout}`}
                                    >
                                      <td>{formatDetailDate(detail.date)}</td>
                                      <td>{formatKeibajo(detail.keibajoCode)}</td>
                                      <td>{formatRaceNumber(detail.raceNumber)}</td>
                                      <td className="stats-detail-race-name">
                                        {detail.raceName || "-"}
                                      </td>
                                      <td>{formatYen(detail.payout)}</td>
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
        </section>

        <section className="stats-category-section">
          <div className="section-heading compact">
            <h3>着順別 人気・オッズ</h3>
          </div>
          <div className="stats-table-wrap">
            <table className="stats-table analysis-table">
              <thead>
                <tr>
                  <th>着順</th>
                  <th>人気 平均</th>
                  <th>人気 中央値</th>
                  <th>オッズ 平均</th>
                  <th>オッズ 中央値</th>
                  <th>件数</th>
                </tr>
              </thead>
              <tbody>
                {finishPositionStats.map((row) => {
                  const isExpanded = expandedFinishKey === row.finishPosition;

                  return (
                    <Fragment key={row.finishPosition}>
                      <tr className={isExpanded ? "stats-row-expanded" : undefined}>
                        <td className="stats-name-cell">
                          {row.details.length > 0 ? (
                            <button
                              aria-expanded={isExpanded}
                              className="stats-detail-toggle"
                              type="button"
                              onClick={() => {
                                setExpandedFinishKey((current) =>
                                  current === row.finishPosition ? null : row.finishPosition,
                                );
                              }}
                            >
                              {row.finishPosition}着
                            </button>
                          ) : (
                            `${row.finishPosition}着`
                          )}
                        </td>
                        <td>{formatNumber(row.averagePopularity)}</td>
                        <td>{formatNumber(row.medianPopularity)}</td>
                        <td>{formatNumber(row.averageOdds)}</td>
                        <td>{formatNumber(row.medianOdds)}</td>
                        <td>{row.count.toLocaleString("ja-JP")}</td>
                      </tr>
                      {isExpanded ? (
                        <tr className="stats-detail-row">
                          <td colSpan={6}>
                            <div className="stats-detail-panel">
                              <table className="stats-detail-table">
                                <thead>
                                  <tr>
                                    <th>日付</th>
                                    <th>競馬場</th>
                                    <th>R</th>
                                    <th>レース名</th>
                                    <th>馬名</th>
                                    <th>騎手</th>
                                    <th>枠</th>
                                    <th>馬番</th>
                                    <th>着順</th>
                                    <th>レースタイム</th>
                                    <th>人気</th>
                                    <th>単勝</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {row.details.map((detail) => (
                                    <tr
                                      key={`${detail.date}-${detail.keibajoCode}-${detail.raceNumber}-${detail.frameNumber}-${detail.horseNumber}-${detail.rank}`}
                                    >
                                      <td>{formatDetailDate(detail.date)}</td>
                                      <td>{formatKeibajo(detail.keibajoCode)}</td>
                                      <td>{formatRaceNumber(detail.raceNumber)}</td>
                                      <td className="stats-detail-race-name">
                                        {detail.raceName || "-"}
                                      </td>
                                      <td className="stats-detail-horse-name">
                                        {detail.horseName || "-"}
                                      </td>
                                      <td>{detail.jockeyName || "-"}</td>
                                      <td>
                                        <FrameNumberBadge value={detail.frameNumber} />
                                      </td>
                                      <td>{detail.horseNumber || "-"}</td>
                                      <td>{parseRank(detail.rank)}</td>
                                      <td>{formatTenthsTime(parseTenths(detail.raceTime))}</td>
                                      <td>{parseRank(detail.popularity)}</td>
                                      <td>{parseOdds(detail.winOdds)}</td>
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
        </section>
      </div>
    </>
  );
});
