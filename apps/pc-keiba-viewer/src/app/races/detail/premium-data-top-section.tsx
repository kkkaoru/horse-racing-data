"use client";

import { memo, useMemo } from "react";

import { getPreferredJockeyName } from "../../../lib/jockey-name";
import type { PremiumDataTopHorse } from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";
import type { RealtimeRaceRequest } from "./realtime-client";
import { useRealtimeRacePayload } from "./realtime-client";

interface PremiumDataTopHorsesTableProps {
  realtimeRequest: RealtimeRaceRequest;
  rows: PremiumDataTopHorse[];
}

interface PremiumDataTopTableRowProps {
  entryStatus: string;
  realtimeJockeyName: string | null | undefined;
  realtimeOdds: number | null | undefined;
  realtimePopularity: number | null | undefined;
  row: PremiumDataTopHorse;
}

const formatPopularity = (value: number | null | undefined): string =>
  value === null || value === undefined ? "-" : `${value}`;

const formatOdds = (value: number | null | undefined): string =>
  value === null || value === undefined ? "-" : value.toFixed(1);

const PremiumDataTopTableRow = memo(function PremiumDataTopTableRow({
  entryStatus,
  realtimeJockeyName,
  realtimeOdds,
  realtimePopularity,
  row,
}: PremiumDataTopTableRowProps) {
  const isScratched = entryStatus !== "";
  const displayedJockey = getPreferredJockeyName(row.jockeyName, realtimeJockeyName ?? null);
  const displayedPopularity = realtimePopularity ?? row.storedPopularity;
  const displayedOdds = realtimeOdds ?? row.storedOdds;

  return (
    <tr
      className={isScratched ? "stats-row-scratched" : undefined}
      data-entry-status={entryStatus || undefined}
    >
      <td>{row.rank}</td>
      <td>{formatRunnerNumber(row.horseNumber)}</td>
      <td className="stats-name-cell">
        {row.horseName ?? "-"}
        {entryStatus ? <span className="runner-status-badge">{entryStatus}</span> : null}
      </td>
      <td className="stats-name-cell">{isScratched ? "-" : displayedJockey || "-"}</td>
      <td>{isScratched ? "-" : formatPopularity(displayedPopularity)}</td>
      <td>{isScratched ? "-" : formatOdds(displayedOdds)}</td>
      <td className="stats-name-cell premium-data-top-reason-cell">
        <ul className="premium-data-top-reasons">
          {row.reasons.map((reason) => (
            <li key={reason}>{reason}</li>
          ))}
        </ul>
      </td>
    </tr>
  );
});

export function PremiumDataTopHorsesTable({ realtimeRequest, rows }: PremiumDataTopHorsesTableProps) {
  const { payload } = useRealtimeRacePayload(realtimeRequest, null);
  const realtimeOddsByHorse = useMemo(
    () =>
      new Map(
        (payload?.odds?.latest.tansho ?? []).map((entry) => [
          formatRunnerNumber(entry.combination),
          { odds: entry.odds ?? null, popularity: entry.rank ?? null },
        ]),
      ),
    [payload],
  );
  const entryStatusByHorse = useMemo(
    () =>
      new Map(
        (payload?.raceEntries?.horses ?? []).map((horse) => [
          formatRunnerNumber(horse.horseNumber),
          horse.status ?? "",
        ]),
      ),
    [payload],
  );
  const realtimeJockeyByHorse = useMemo(
    () =>
      new Map(
        (payload?.raceEntries?.horses ?? []).map((horse) => [
          formatRunnerNumber(horse.horseNumber),
          horse.jockeyName ?? "",
        ]),
      ),
    [payload],
  );

  if (rows.length === 0) {
    return null;
  }

  return (
    <div className="stats-table-wrap premium-data-top-table-wrap">
      <table className="stats-table premium-data-top-table">
        <colgroup>
          <col className="premium-data-top-col-rank" />
          <col className="premium-data-top-col-number" />
          <col className="premium-data-top-col-horse" />
          <col className="premium-data-top-col-jockey" />
          <col className="premium-data-top-col-popularity" />
          <col className="premium-data-top-col-odds" />
          <col className="premium-data-top-col-reason" />
        </colgroup>
        <thead>
          <tr>
            <th>順位</th>
            <th>馬番</th>
            <th>馬名</th>
            <th>騎手名</th>
            <th>人気</th>
            <th>単勝</th>
            <th>理由</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const horseNumber = formatRunnerNumber(row.horseNumber);
            const realtimeOdds = realtimeOddsByHorse.get(horseNumber);
            return (
              <PremiumDataTopTableRow
                key={`${row.rank}-${horseNumber}`}
                entryStatus={entryStatusByHorse.get(horseNumber) ?? ""}
                realtimeJockeyName={realtimeJockeyByHorse.get(horseNumber)}
                realtimeOdds={realtimeOdds?.odds}
                realtimePopularity={realtimeOdds?.popularity}
                row={row}
              />
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
