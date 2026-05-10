"use client";

import { useRouter, useSearchParams } from "next/navigation";
import type { ReactNode } from "react";
import { Fragment, useState } from "react";

import { formatDate, formatKeibajo, formatRaceNumber } from "../../../lib/format";
import type {
  FinishPositionStatsRow,
  FrameStatsRow,
  PayoutStatsRow,
  RaceTimeStats,
  SimilarRaceStatsSettings,
  StatsDetail,
} from "../../../lib/race-types";

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
  settings: SimilarRaceStatsSettings;
}

type ToggleSettingKey = keyof Omit<
  SimilarRaceStatsSettings,
  "classConditionName" | "runnerCount" | "years"
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
  includeRunnerCount: "statsRunnerCount",
  includeSex: "statsSex",
  includeSurface: "statsSurface",
  includeTurn: "statsTurn",
  includeVenue: "statsVenue",
  includeWeight: "statsWeight",
};

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
            <td>{detail.frameNumber || "-"}</td>
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

export function RaceConditionAnalysisSection({
  conditionLabels,
  frameStats,
  finishPositionStats,
  payoutStats,
  raceTimeStats,
  settings,
}: RaceConditionAnalysisSectionProps) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [expandedPayoutKey, setExpandedPayoutKey] = useState<string | null>(null);
  const [expandedFinishKey, setExpandedFinishKey] = useState<number | null>(null);
  const [expandedFrameKey, setExpandedFrameKey] = useState<string | null>(null);

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
  const sortedFrameStats = frameStats.toSorted(
    (left, right) =>
      right.score - left.score ||
      right.count - left.count ||
      left.frameNumber.localeCompare(right.frameNumber),
  );

  return (
    <>
      <section className="stats-control-panel" aria-label="race condition analysis controls">
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
        {renderConditionToggle("includeRunnerCount", conditionLabels.runnerCount)}
        {renderConditionToggle("includeFrame", conditionLabels.frame)}
        {renderConditionToggle("includeRaceNumber", conditionLabels.raceNumber)}
      </section>

      <div className="stats-category-list">
        <section className="stats-category-section">
          <div className="section-heading compact">
            <h3>タイム傾向</h3>
            <span>{raceTimeStats.raceCount.toLocaleString("ja-JP")} 件</span>
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
            <h3>枠順分析</h3>
            <span>{frameStats.length} 枠</span>
          </div>
          <div className="stats-table-wrap">
            <table className="stats-table analysis-table">
              <thead>
                <tr>
                  <th>枠</th>
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

                  return (
                    <Fragment key={row.frameNumber}>
                      <tr className={isExpanded ? "stats-row-expanded" : undefined}>
                        <td className="stats-name-cell">
                          {row.details.length > 0 ? (
                            <button
                              aria-expanded={isExpanded}
                              className="stats-detail-toggle"
                              type="button"
                              onClick={() => {
                                setExpandedFrameKey((current) =>
                                  current === row.frameNumber ? null : row.frameNumber,
                                );
                              }}
                            >
                              {row.frameNumber}枠
                            </button>
                          ) : (
                            `${row.frameNumber}枠`
                          )}
                        </td>
                        <td>{runnerCountLabel}</td>
                        <td className="stats-score-cell">{row.score.toFixed(2)}</td>
                        <td>{formatNumber(row.averageFinish)}</td>
                        <td>{formatNumber(row.medianFinish)}</td>
                        <td>{formatNumber(row.averagePopularity)}</td>
                        <td>{formatNumber(row.medianPopularity)}</td>
                      </tr>
                      {isExpanded ? (
                        <tr className="stats-detail-row">
                          <td colSpan={7}>
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
                                      <td>{detail.frameNumber || "-"}</td>
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
            <span>{payoutStats.length} 種</span>
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
            <span>{finishPositionStats.length} 着順</span>
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
                                      <td>{detail.frameNumber || "-"}</td>
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
}
