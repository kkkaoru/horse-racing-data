"use client";

import { Fragment, useMemo, useState } from "react";

import { getPreferredJockeyName } from "../../../lib/jockey-name";
import type {
  ConditionCorrelationDetail,
  ConditionCorrelationRow,
  TimeScoreDetail,
  TimeScoreRow,
} from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";
import type { RealtimeRaceRequest } from "./realtime-client";
import { useRealtimeRacePayload } from "./realtime-client";

interface TimeScoreTableProps {
  correlationRows: ConditionCorrelationRow[];
  realtimeRequest?: RealtimeRaceRequest;
  rows: TimeScoreRow[];
}

interface CombinedScoreRow {
  correlationDetails: ConditionCorrelationDetail[];
  correlationScore: number;
  horseName: string;
  horseNumber: string;
  jockeyName: string;
  rawScore: number;
  timeDetails: TimeScoreDetail[];
  timeScore: number;
  totalScore: number;
}

const normalizeHorseNumber = (value: string): string =>
  value.replace(/^0+/u, "") || (value ? "0" : "");

const roundScore = (value: number): number => Math.round(value * 100) / 100;

const clampScore = (value: number): number => Math.max(0, Math.min(1, value));

const formatDetailNumber = (value: number | null): string =>
  value === null ? "-" : value.toFixed(1);

const similarityScore = (value: number | null, target: number | null, scale: number): number => {
  if (value === null || target === null) {
    return 0.5;
  }
  return Math.max(0, Math.min(1, 1 - Math.abs(value - target) / Math.max(target, scale)));
};

const applyRealtimeCorrelationRows = (
  rows: ConditionCorrelationRow[],
  realtimeValues: Map<string, { odds: number | null; popularity: number | null }>,
): ConditionCorrelationRow[] => {
  if (realtimeValues.size === 0) {
    return rows;
  }

  return rows.map((row) => {
    const realtime = realtimeValues.get(normalizeHorseNumber(row.horseNumber));
    if (!realtime) {
      return row;
    }
    const details = row.details.map((detail): ConditionCorrelationDetail => {
      if (detail.key === "popularity" && realtime.popularity !== null) {
        return Object.assign({}, detail, {
          reason: `${detail.reason}。最新オッズ取得値で再計算`,
          score: roundScore(similarityScore(realtime.popularity, detail.target, 5)),
          value: realtime.popularity,
        });
      }
      if (detail.key === "odds" && realtime.odds !== null) {
        return Object.assign({}, detail, {
          reason: `${detail.reason}。最新オッズ取得値で再計算`,
          score: roundScore(similarityScore(realtime.odds, detail.target, 10)),
          value: realtime.odds,
        });
      }
      return detail;
    });
    const score = roundScore(
      details.reduce((total, detail) => total + detail.score * detail.weight, 0),
    );
    return Object.assign({}, row, { details, score });
  });
};

const buildCombinedRows = (
  timeRows: TimeScoreRow[],
  correlationRows: ConditionCorrelationRow[],
): CombinedScoreRow[] => {
  const correlationByHorse = new Map(
    correlationRows.map((row) => [normalizeHorseNumber(row.horseNumber), row]),
  );
  const rawRows = timeRows.map((row) => {
    const horseNumber = normalizeHorseNumber(row.horseNumber);
    const correlationRow = correlationByHorse.get(horseNumber);
    const timeScore = clampScore(row.score);
    const correlationScore = clampScore(correlationRow?.score ?? 0.5);
    const rawScore = (timeScore + correlationScore) / 2;
    return {
      correlationDetails: correlationRow?.details ?? [],
      correlationScore,
      horseName: row.horseName,
      horseNumber: row.horseNumber,
      jockeyName: row.jockeyName,
      rawScore,
      timeDetails: row.details,
      timeScore,
      totalScore: 0,
    };
  });
  if (rawRows.length === 0) {
    return [];
  }

  const minScore = Math.min(...rawRows.map((row) => row.rawScore));
  const maxScore = Math.max(...rawRows.map((row) => row.rawScore));
  const sortedFallbackRows = rawRows.toSorted(
    (left, right) => Number(left.horseNumber) - Number(right.horseNumber),
  );
  for (const row of rawRows) {
    if (rawRows.length === 1) {
      row.totalScore = 1;
      continue;
    }
    if (maxScore === minScore) {
      const fallbackIndex = sortedFallbackRows.findIndex(
        (fallbackRow) => fallbackRow.horseNumber === row.horseNumber,
      );
      if (fallbackIndex === 0) {
        row.totalScore = 1;
      } else if (fallbackIndex === sortedFallbackRows.length - 1) {
        row.totalScore = 0;
      } else {
        row.totalScore = 0.5;
      }
      continue;
    }
    row.totalScore = (row.rawScore - minScore) / (maxScore - minScore);
  }
  return rawRows.toSorted(
    (left, right) =>
      right.totalScore - left.totalScore || Number(left.horseNumber) - Number(right.horseNumber),
  );
};

export function TimeScoreTable({ correlationRows, realtimeRequest, rows }: TimeScoreTableProps) {
  const [expandedHorseNumber, setExpandedHorseNumber] = useState<string | null>(null);
  const { payload } = useRealtimeRacePayload(
    realtimeRequest ?? {
      apiBaseUrl: "",
      day: "",
      keibajoCode: "",
      month: "",
      raceNumber: "",
      source: "jra",
      year: "",
    },
    null,
  );
  const realtimeValues = useMemo(() => {
    const values = new Map<string, { odds: number | null; popularity: number | null }>();
    for (const row of payload?.odds?.latest.tansho ?? []) {
      values.set(normalizeHorseNumber(row.combination), {
        odds: row.odds ?? null,
        popularity: row.rank ?? null,
      });
    }
    return values;
  }, [payload]);
  const realtimeJockeyByHorse = useMemo(
    () =>
      new Map(
        (payload?.raceEntries?.horses ?? []).map((horse) => [
          normalizeHorseNumber(horse.horseNumber),
          horse.jockeyName ?? "",
        ]),
      ),
    [payload],
  );
  const displayedRows = useMemo(
    () => buildCombinedRows(rows, applyRealtimeCorrelationRows(correlationRows, realtimeValues)),
    [correlationRows, realtimeValues, rows],
  );

  return (
    <div className="stats-table-wrap time-score-table-wrap">
      <table className="stats-table analysis-table time-score-table">
        <thead>
          <tr>
            <th>馬番</th>
            <th>馬名</th>
            <th>騎手名</th>
            <th>合計スコア</th>
            <th>タイムスコア</th>
            <th>1〜3着相関スコア</th>
          </tr>
        </thead>
        <tbody>
          {displayedRows.length > 0 ? (
            displayedRows.map((row) => {
              const isExpanded = expandedHorseNumber === row.horseNumber;
              return (
                <Fragment key={row.horseNumber}>
                  <tr className={isExpanded ? "stats-row-expanded" : undefined}>
                    <td>{formatRunnerNumber(row.horseNumber)}</td>
                    <td className="stats-name-cell">{row.horseName || "-"}</td>
                    <td className="stats-name-cell">
                      {getPreferredJockeyName(
                        row.jockeyName,
                        realtimeJockeyByHorse.get(normalizeHorseNumber(row.horseNumber)),
                      ) || "-"}
                    </td>
                    <td className="stats-score-cell">
                      <button
                        aria-expanded={isExpanded}
                        className="stats-detail-toggle"
                        type="button"
                        onClick={() => {
                          setExpandedHorseNumber((current) =>
                            current === row.horseNumber ? null : row.horseNumber,
                          );
                        }}
                      >
                        {row.totalScore.toFixed(2)}
                      </button>
                    </td>
                    <td>{row.timeScore.toFixed(2)}</td>
                    <td>{row.correlationScore.toFixed(2)}</td>
                  </tr>
                  {isExpanded ? (
                    <tr className="stats-detail-row time-score-detail-row">
                      <td colSpan={6}>
                        <div className="stats-detail-panel">
                          <table className="stats-detail-table time-score-detail-table">
                            <thead>
                              <tr>
                                <th>種別</th>
                                <th>項目</th>
                                <th>現在値</th>
                                <th>対象平均</th>
                                <th>スコア</th>
                                <th>重み</th>
                                <th>理由</th>
                              </tr>
                            </thead>
                            <tbody>
                              {row.timeDetails.map((detail) => (
                                <tr key={`time-${detail.label}`}>
                                  <td>タイム</td>
                                  <td>{detail.label}</td>
                                  <td>{formatDetailNumber(detail.value)}</td>
                                  <td>{formatDetailNumber(detail.target)}</td>
                                  <td>{detail.score.toFixed(2)}</td>
                                  <td>{detail.weight.toFixed(3)}</td>
                                  <td className="stats-name-cell">{detail.reason}</td>
                                </tr>
                              ))}
                              {row.correlationDetails.map((detail) => (
                                <tr key={`correlation-${detail.key}`}>
                                  <td>1〜3着相関</td>
                                  <td>{detail.label}</td>
                                  <td>{formatDetailNumber(detail.value)}</td>
                                  <td>{formatDetailNumber(detail.target)}</td>
                                  <td>{detail.score.toFixed(2)}</td>
                                  <td>{detail.weight.toFixed(3)}</td>
                                  <td className="stats-name-cell">{detail.reason}</td>
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
            })
          ) : (
            <tr>
              <td colSpan={6}>タイムスコアを表示できるデータがありません。</td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
