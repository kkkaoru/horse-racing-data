// Runs with Bun for tests and in the browser behind the existing Access origin.
"use client";

import { useEffect, useState, type ChangeEvent } from "react";

import {
  BET_BUDGET_STEP,
  DEFAULT_BET_BUDGET,
  MIN_BET_BUDGET,
  buildClefBetsUrl,
  formatOdds,
  formatPayout,
  formatYen,
  normalizeBudget,
  readClefBets,
  summarizeSuggestion,
  type ClefBetCard,
  type ClefBetRunner,
} from "../../../lib/clef-bets";
import type { ClefRerankRaceAddress } from "../../../lib/clef-rerank";

interface ClefBetState {
  url: string;
  card: ClefBetCard | null;
}

interface BetBodyProps {
  state: ClefBetState | null;
  url: string;
}

interface HorseCellsProps {
  horses: readonly number[];
  runners: ReadonlyMap<number, ClefBetRunner>;
  pick: (runner: ClefBetRunner) => string;
}

const SLIDER_MAX_BUDGET: number = 50_000;
const FETCH_DELAY_MS: number = 400;
const MISSING: string = "-";

const loadCard = async (url: string, signal: AbortSignal): Promise<ClefBetCard | null> => {
  try {
    const response = await fetch(url, { credentials: "same-origin", signal, cache: "no-store" });
    return response.ok ? readClefBets(await response.json()) : null;
  } catch {
    return null;
  }
};

function HorseCells({ horses, runners, pick }: HorseCellsProps) {
  return (
    <td>
      {horses.map((horse) => {
        const runner = runners.get(horse);
        return <div key={horse}>{runner === undefined ? MISSING : pick(runner)}</div>;
      })}
    </td>
  );
}

function BetBody({ state, url }: BetBodyProps) {
  if (state === null || state.url !== url) return <p>推奨馬券を計算しています…</p>;
  const card = state.card;
  if (card === null) return <p>オッズ発表後に推奨馬券を表示します。</p>;
  const runners = new Map(card.runners.map((runner) => [runner.horseNumber, runner]));
  return (
    <>
      <p>{summarizeSuggestion(card.suggestion)}</p>
      <table className="runner-table">
        <caption>オッズ取得: {card.oddsFetchedAt}</caption>
        <thead>
          <tr>
            <th scope="col">枠番</th>
            <th scope="col">馬番</th>
            <th scope="col">馬名</th>
            <th scope="col">騎手</th>
            <th scope="col">調教師</th>
            <th scope="col">馬主</th>
            <th scope="col">オッズ</th>
            <th scope="col">購入金額</th>
            <th scope="col">払戻額</th>
          </tr>
        </thead>
        <tbody>
          {card.suggestion.tickets.map((ticket) => (
            <tr key={ticket.horses.join("-")}>
              <HorseCells
                horses={ticket.horses}
                pick={(runner) => String(runner.frameNumber)}
                runners={runners}
              />
              <HorseCells
                horses={ticket.horses}
                pick={(runner) => String(runner.horseNumber)}
                runners={runners}
              />
              <HorseCells
                horses={ticket.horses}
                pick={(runner) => runner.horseName}
                runners={runners}
              />
              <HorseCells
                horses={ticket.horses}
                pick={(runner) => runner.jockey}
                runners={runners}
              />
              <HorseCells
                horses={ticket.horses}
                pick={(runner) => runner.trainer}
                runners={runners}
              />
              <HorseCells
                horses={ticket.horses}
                pick={(runner) => runner.owner}
                runners={runners}
              />
              <td>{formatOdds(ticket)}</td>
              <td>{formatYen(ticket.stake)}</td>
              <td>{formatPayout(ticket)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}

// Shows clef-flash bet suggestions under the realtime odds. The budget can be slid in 100-yen
// steps or typed; the Worker reuses one clef-flash answer per odds snapshot for every budget.
export function ClefBetSection(race: ClefRerankRaceAddress) {
  const [budget, setBudget] = useState<number>(DEFAULT_BET_BUDGET);
  const [draft, setDraft] = useState<string>(String(DEFAULT_BET_BUDGET));
  const [state, setState] = useState<ClefBetState | null>(null);
  const url = buildClefBetsUrl(race, budget);
  useEffect(() => {
    const controller = new AbortController();
    const timer = setTimeout(() => {
      const load = async (): Promise<void> => {
        const card = await loadCard(url, controller.signal);
        if (!controller.signal.aborted) setState({ url, card });
      };
      void load();
    }, FETCH_DELAY_MS);
    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [url]);
  const onSlide = (event: ChangeEvent<HTMLInputElement>): void => {
    const next = normalizeBudget(Number(event.target.value));
    setBudget(next);
    setDraft(String(next));
  };
  // Typed values apply only once they are on the 100-yen grid, so partial input stays editable.
  const onType = (event: ChangeEvent<HTMLInputElement>): void => {
    const value = Number(event.target.value);
    setDraft(event.target.value);
    if (event.target.value !== "" && normalizeBudget(value) === value) setBudget(value);
  };
  return (
    <section aria-label="clef-flash 推奨馬券" className="clef-bet-section">
      <h3>clef-flash 推奨馬券</h3>
      <div className="clef-bet-budget">
        <label>
          予算
          <input
            aria-label="予算スライダー"
            max={SLIDER_MAX_BUDGET}
            min={MIN_BET_BUDGET}
            onChange={onSlide}
            step={BET_BUDGET_STEP}
            type="range"
            value={Math.min(budget, SLIDER_MAX_BUDGET)}
          />
        </label>
        <input
          aria-label="予算（円）"
          min={MIN_BET_BUDGET}
          onChange={onType}
          step={BET_BUDGET_STEP}
          type="number"
          value={draft}
        />
        <span>円</span>
      </div>
      <BetBody state={state} url={url} />
    </section>
  );
}
