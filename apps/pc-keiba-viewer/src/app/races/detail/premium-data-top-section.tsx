"use client";

import type { PremiumDataTopHorse } from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";

interface PremiumDataTopHorsesTableProps {
  rows: PremiumDataTopHorse[];
}

export function PremiumDataTopHorsesTable({ rows }: PremiumDataTopHorsesTableProps) {
  if (rows.length === 0) {
    return null;
  }

  return (
    <div className="premium-data-top-panel">
      <table className="stats-table premium-data-top-table">
        <thead>
          <tr>
            <th>順位</th>
            <th>馬番</th>
            <th>馬名</th>
            <th>理由</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={`${row.rank}-${row.horseNumber}`}>
              <td>{row.rank}</td>
              <td>{formatRunnerNumber(row.horseNumber)}</td>
              <td>{row.horseName ?? "-"}</td>
              <td>
                <ul className="premium-data-top-reasons">
                  {row.reasons.map((reason) => (
                    <li key={reason}>{reason}</li>
                  ))}
                </ul>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
