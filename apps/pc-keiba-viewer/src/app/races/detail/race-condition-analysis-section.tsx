"use client";

import Link from "next/link";
import { Fragment, memo, useEffect, useState } from "react";

import { formatDate, formatKeibajo, formatRaceNumber } from "../../../lib/format";
import type {
  FinishPositionStatsRow,
  PayoutStatsRow,
  RaceTimeStats,
} from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";
import { FrameNumberBadge } from "./frame-number-badge";
import { formatRaceTimeDecimalTenths, formatRaceTimeTenths } from "./race-time-stats-metrics";

interface RaceConditionAnalysisSectionProps {
  finishPositionStats: FinishPositionStatsRow[];
  payoutStats: PayoutStatsRow[];
  raceTimeStats: RaceTimeStats;
}

const TARGET_RACE_PAGE_SIZE_OPTIONS = [5, 10, 20, 50];

const formatDetailDate = (date: string): string =>
  date.length === 8 ? formatDate(date.slice(0, 4), date.slice(4, 8)) : "-";

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

export const RaceConditionAnalysisSection = memo(function RaceConditionAnalysisSection({
  finishPositionStats,
  payoutStats,
  raceTimeStats,
}: RaceConditionAnalysisSectionProps) {
  const [expandedPayoutKey, setExpandedPayoutKey] = useState<string | null>(null);
  const [expandedFinishKey, setExpandedFinishKey] = useState<number | null>(null);
  const [targetRacePage, setTargetRacePage] = useState(1);
  const [targetRacePageSize, setTargetRacePageSize] = useState(5);

  useEffect(() => {
    setTargetRacePage(1);
  }, [raceTimeStats.targetRaces, targetRacePageSize]);

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
      <div className="stats-category-list">
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
                      <td>{formatRaceTimeTenths(parseTenths(targetRace.raceTime))}</td>
                      <td>{formatRaceTimeDecimalTenths(parseTenths(targetRace.kohan3f))}</td>
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
                          <td aria-label="払戻分析の詳細" colSpan={6}>
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
                          <td aria-label="オッズ分析の詳細" colSpan={6}>
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
                                      <td>{formatRaceTimeTenths(parseTenths(detail.raceTime))}</td>
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
