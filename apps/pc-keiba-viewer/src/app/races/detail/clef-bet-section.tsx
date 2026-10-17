// Runs with Bun for tests and in the browser behind the existing Access origin.
"use client";

import { useEffect, useId, useState, type ChangeEvent, type KeyboardEvent } from "react";

import {
  BET_BUDGET_STEP,
  BET_COLUMNS_STORAGE_KEY,
  OPTIONAL_BET_COLUMNS,
  DEFAULT_BET_BUDGET,
  MAX_BET_BUDGET,
  MIN_BET_BUDGET,
  betStyleLabel,
  betTypeLabel,
  buildClefBetsUrl,
  describePlan,
  formatPlanOdds,
  formatPlanPayout,
  formatPlanStake,
  formatYen,
  normalizeBudget,
  planGroups,
  readClefBets,
  readHiddenBetColumns,
  summarizePlanSet,
  trimName,
  type ClefBetCard,
  type ClefBetRunner,
  type ClefBetSuggestion,
} from "../../../lib/clef-bets";
import type { ClefRerankRaceAddress } from "../../../lib/clef-rerank";
import { useRealtimeRaceSelector } from "./realtime-client";

interface ClefBetState {
  url: string;
  card: ClefBetCard | null;
}

interface BetBodyProps {
  state: ClefBetState | null;
  hidden: readonly string[];
}

interface ColumnTogglesProps {
  hidden: readonly string[];
  onToggle: (label: string, visible: boolean) => void;
}

interface GroupCellProps {
  plan: ClefBetSuggestion;
  runners: ReadonlyMap<number, ClefBetRunner>;
  pick: (runner: ClefBetRunner) => string;
}

interface BudgetControlProps {
  budget: number;
  onChange: (budget: number) => void;
}

interface PickColumn {
  label: string;
  pick: (runner: ClefBetRunner) => string;
}

const SLIDER_MAX_BUDGET: number = 20_000;
const LARGE_STEP: number = 1000;
const FETCH_DELAY_MS: number = 400;
const MISSING: string = "-";
const MOBILE_QUERY: string = "(max-width: 760px)";
const PICK_COLUMNS: readonly PickColumn[] = [
  { label: "枠番", pick: (runner) => String(runner.frameNumber) },
  { label: "馬番", pick: (runner) => String(runner.horseNumber) },
  { label: "馬名", pick: (runner) => trimName(runner.horseName) },
  { label: "騎手", pick: (runner) => trimName(runner.jockey) },
  { label: "調教師", pick: (runner) => trimName(runner.trainer) },
  { label: "馬主", pick: (runner) => trimName(runner.owner) },
];

const loadCard = async (url: string, signal: AbortSignal): Promise<ClefBetCard | null> => {
  try {
    const response = await fetch(url, { credentials: "same-origin", signal, cache: "no-store" });
    return response.ok ? readClefBets(await response.json()) : null;
  } catch {
    return null;
  }
};

// One block per leg (軸 / 相手, 1着 / 2着 / 3着, ボックス) so nagashi, multi and formation
// tickets read as a betting slip instead of one row per combination.
function GroupCell({ plan, runners, pick }: GroupCellProps) {
  return (
    <td>
      {planGroups(plan).map((group) => (
        <div className="clef-bet-leg" key={group.label}>
          {group.label === "" ? null : <span className="clef-bet-leg-label">{group.label}</span>}
          {group.horses.map((horse) => {
            const runner = runners.get(horse);
            return <div key={horse}>{runner === undefined ? MISSING : pick(runner)}</div>;
          })}
        </div>
      ))}
    </td>
  );
}

// Same control as the 競走成績 filters; the choice is kept on this device for every race.
function ColumnToggles({ hidden, onToggle }: ColumnTogglesProps) {
  return (
    <fieldset className="clef-bet-columns">
      <legend>表示する列</legend>
      {OPTIONAL_BET_COLUMNS.map((label) => (
        <label className="race-results-checkbox-label" key={label}>
          <span>{label}</span>
          <span className="race-results-checkbox-control">
            <input
              aria-label={label}
              checked={!hidden.includes(label)}
              onChange={(event) => onToggle(label, event.currentTarget.checked)}
              type="checkbox"
            />
          </span>
        </label>
      ))}
    </fieldset>
  );
}

const readStoredColumns = (): string | null => {
  try {
    return window.localStorage.getItem(BET_COLUMNS_STORAGE_KEY);
  } catch {
    return null;
  }
};

const writeStoredColumns = (hidden: readonly string[]): void => {
  try {
    window.localStorage.setItem(BET_COLUMNS_STORAGE_KEY, JSON.stringify(hidden));
  } catch {
    // Private mode or blocked storage only loses the remembered choice.
  }
};

function BetBody({ state, hidden }: BetBodyProps) {
  if (state === null) return <p>推奨馬券を計算しています…</p>;
  const card = state.card;
  if (card === null) return <p>オッズ発表後に推奨馬券を表示します。</p>;
  const runners = new Map(card.runners.map((runner) => [runner.horseNumber, runner]));
  const columns = PICK_COLUMNS.filter((column) => !hidden.includes(column.label));
  return (
    <>
      <p>{summarizePlanSet(card.plans)}</p>
      <div className="runner-table-wrap">
        <table className="clef-bet-table">
          <caption>オッズ取得: {card.oddsFetchedAt}</caption>
          <thead>
            <tr>
              <th scope="col">馬券</th>
              {columns.map((column) => (
                <th key={column.label} scope="col">
                  {column.label}
                </th>
              ))}
              <th scope="col">オッズ</th>
              <th scope="col">購入金額</th>
              <th scope="col">払戻額</th>
            </tr>
          </thead>
          <tbody>
            {card.plans.plans.map((plan) => (
              <tr key={`${plan.betType}-${plan.style}`}>
                <th scope="row">
                  <span className="clef-bet-type">{betTypeLabel(plan.betType)}</span>
                  <span className="clef-bet-style">{betStyleLabel(plan.style)}</span>
                  <span className="clef-bet-structure">{describePlan(plan)}</span>
                </th>
                {columns.map((column) => (
                  <GroupCell key={column.label} pick={column.pick} plan={plan} runners={runners} />
                ))}
                <td className="clef-bet-number">{formatPlanOdds(plan)}</td>
                <td className="clef-bet-number">{formatPlanStake(plan)}</td>
                <td className="clef-bet-number">{formatPlanPayout(plan)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

// Budget entry: a labelled slider (arrow keys move 100 yen, PageUp/PageDown 1,000 yen), ±buttons
// and a numeric field that applies on blur or Enter so partial typing never snaps away.
function BudgetControl({ budget, onChange }: BudgetControlProps) {
  const id = useId();
  const [draft, setDraft] = useState<string>(String(budget));
  useEffect(() => setDraft(String(budget)), [budget]);
  const commit = (): void => {
    const next = normalizeBudget(Number(draft));
    setDraft(String(next));
    onChange(next);
  };
  const onSlide = (event: ChangeEvent<HTMLInputElement>): void =>
    onChange(normalizeBudget(Number(event.target.value)));
  const onKey = (event: KeyboardEvent<HTMLInputElement>): void => {
    if (event.key === "Enter") commit();
  };
  return (
    <fieldset className="clef-bet-budget">
      <legend>予算</legend>
      <output aria-live="polite" className="clef-bet-budget-value" htmlFor={`${id}-range`}>
        {formatYen(budget)}
      </output>
      <div className="clef-bet-budget-controls">
        <button
          aria-label="予算を1,000円減らす"
          disabled={budget <= MIN_BET_BUDGET}
          onClick={() => onChange(normalizeBudget(budget - LARGE_STEP))}
          type="button"
        >
          −1,000
        </button>
        <button
          aria-label="予算を100円減らす"
          disabled={budget <= MIN_BET_BUDGET}
          onClick={() => onChange(normalizeBudget(budget - BET_BUDGET_STEP))}
          type="button"
        >
          −100
        </button>
        <input
          aria-label="予算スライダー"
          aria-valuetext={formatYen(budget)}
          id={`${id}-range`}
          max={SLIDER_MAX_BUDGET}
          min={MIN_BET_BUDGET}
          onChange={onSlide}
          step={BET_BUDGET_STEP}
          type="range"
          value={Math.min(budget, SLIDER_MAX_BUDGET)}
        />
        <button
          aria-label="予算を100円増やす"
          disabled={budget >= MAX_BET_BUDGET}
          onClick={() => onChange(normalizeBudget(budget + BET_BUDGET_STEP))}
          type="button"
        >
          ＋100
        </button>
        <button
          aria-label="予算を1,000円増やす"
          disabled={budget >= MAX_BET_BUDGET}
          onClick={() => onChange(normalizeBudget(budget + LARGE_STEP))}
          type="button"
        >
          ＋1,000
        </button>
      </div>
      <div className="clef-bet-budget-input">
        <label htmlFor={`${id}-number`}>金額を入力（100円単位）</label>
        <input
          id={`${id}-number`}
          inputMode="numeric"
          max={MAX_BET_BUDGET}
          min={MIN_BET_BUDGET}
          onBlur={commit}
          onChange={(event) => setDraft(event.target.value)}
          onKeyDown={onKey}
          step={BET_BUDGET_STEP}
          type="number"
          value={draft}
        />
        <span aria-hidden="true">円</span>
      </div>
    </fieldset>
  );
}

// Shows suggested tickets under the realtime odds. The page polls realtime odds only while it is
// open, so suggestions follow each new snapshot only for viewers; the Worker re-runs clef-flash
// only when the favourites' order or the served prediction changes.
export function ClefBetSection(race: ClefRerankRaceAddress) {
  const [budget, setBudget] = useState<number>(DEFAULT_BET_BUDGET);
  const [state, setState] = useState<ClefBetState | null>(null);
  const [hidden, setHidden] = useState<string[]>([]);
  useEffect(() => {
    const mobile =
      typeof window.matchMedia === "function" && window.matchMedia(MOBILE_QUERY).matches;
    setHidden(readHiddenBetColumns(readStoredColumns(), mobile));
  }, []);
  const onToggle = (label: string, visible: boolean): void => {
    const next = visible ? hidden.filter((item) => item !== label) : [...hidden, label];
    setHidden(next);
    writeStoredColumns(next);
  };
  const oddsFetchedAt = useRealtimeRaceSelector((store) => store.payload?.odds?.fetchedAt ?? "");
  const url = `${buildClefBetsUrl(race, budget)}&odds=${encodeURIComponent(oddsFetchedAt)}`;
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
  return (
    <section aria-label="推奨馬券" className="clef-bet-section">
      <h3>推奨馬券</h3>
      <BudgetControl budget={budget} onChange={setBudget} />
      <ColumnToggles hidden={hidden} onToggle={onToggle} />
      <BetBody hidden={hidden} state={state} />
    </section>
  );
}
