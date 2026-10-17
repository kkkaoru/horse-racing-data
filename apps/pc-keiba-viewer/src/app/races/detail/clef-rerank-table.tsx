// Runs with Bun for tests and in the browser behind the existing Access origin.
"use client";

import { useEffect, useState } from "react";

import {
  buildClefRerankUrl,
  formatRankChange,
  readClefRerank,
  type ClefRerankCard,
  type ClefRerankRaceAddress,
} from "../../../lib/clef-rerank";
import type { Runner } from "../../../lib/race-types";

type ClefRerankRunner = Pick<Runner, "umaban" | "bamei">;

interface ClefRerankTableProps extends ClefRerankRaceAddress {
  runners: readonly ClefRerankRunner[];
}

interface ClefRerankState {
  url: string;
  card: ClefRerankCard | null;
}

const PERCENT: number = 100;
const PERCENT_DIGITS: number = 1;

const formatPercent = (value: number): string => `${(value * PERCENT).toFixed(PERCENT_DIGITS)}%`;

const runnerName = (runners: readonly ClefRerankRunner[], horseNumber: number): string =>
  runners.find((runner) => Number(runner.umaban) === horseNumber)?.bamei ?? "-";

const loadCard = async (url: string, signal: AbortSignal): Promise<ClefRerankCard | null> => {
  try {
    const response = await fetch(url, { credentials: "same-origin", signal, cache: "no-store" });
    return response.ok ? readClefRerank(await response.json()) : null;
  } catch {
    return null;
  }
};

// Shows the clef-flash reranked order above the served model table. Missing
// bundles, Worker errors and malformed payloads hide the panel entirely.
export function ClefRerankTable({ runners, ...race }: ClefRerankTableProps) {
  const url = buildClefRerankUrl(race);
  const [state, setState] = useState<ClefRerankState | null>(null);
  useEffect(() => {
    const controller = new AbortController();
    const load = async (): Promise<void> => {
      const card = await loadCard(url, controller.signal);
      if (!controller.signal.aborted) setState({ url, card });
    };
    void load();
    return () => controller.abort();
  }, [url]);

  if (state === null || state.url !== url || state.card === null) return null;
  const card = state.card;
  return (
    <section aria-label="clef-flash 補正予測" className="clef-rerank-section">
      <h3>{card.label} 着順予測</h3>
      <p>
        配信中のモデル（{card.baseModelVersion}
        ）の順位に、騎手・厩舎・馬主・枠の成績、血統の組み合わせとコース相性、Cloudflare clef-flash
        の判定を合成した順位です。clef の確率は較正前の推定値です。
      </p>
      <table className="runner-table">
        <caption>
          モデル: {card.modelVersion} ／ 元予測の生成: {card.baseGeneratedAt}
        </caption>
        <thead>
          <tr>
            <th scope="col">補正順位</th>
            <th scope="col">馬番</th>
            <th scope="col">馬名</th>
            <th scope="col">元の順位</th>
            <th scope="col">変動</th>
            <th scope="col">clef 勝率</th>
            <th scope="col">clef 3着内</th>
          </tr>
        </thead>
        <tbody>
          {card.predictions.map((row) => (
            <tr key={row.horseNumber}>
              <td>{row.rank}</td>
              <td>{row.horseNumber}</td>
              <td>{runnerName(runners, row.horseNumber)}</td>
              <td>{row.baseRank}</td>
              <td>{formatRankChange(row)}</td>
              <td>{formatPercent(row.clefWin)}</td>
              <td>{formatPercent(row.clefTop3)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}
